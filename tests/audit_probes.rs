//! Audit probes. Each one states a property the daemon claims to keep and
//! checks it against a concrete input. A failure is the reproduction.

mod common;
use common::*;

use std::io::{Read, Write};
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{fs, thread};

fn marker() -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    format!(
        "sleep {}",
        700_000_000u64 + std::process::id() as u64 * 100 + n as u64
    )
}

/// SIGKILL anything whose cmdline contains `needle`, including after a panic.
struct Reap(String);

impl Drop for Reap {
    fn drop(&mut self) {
        for pid in pids_matching(&self.0) {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).output();
        }
    }
}

/// `kill` snapshots /proc once, signals that set, then signals the leader.
/// A child forked after the snapshot keeps the session id. Plain `sleep &`
/// still dies, because leader exit sends SIGHUP to that process group; a
/// child that ignores SIGHUP does not. The probe forks such children in a
/// loop so some are born inside that window.
#[test]
fn kill_during_fork_loop_leaves_no_session_orphans() {
    let needle = marker();
    let number = needle.split_whitespace().nth(1).unwrap();
    let _reap = Reap(needle.clone());
    let d = Daemon::start();
    let script = d.dir.join("forkloop.py");
    fs::write(
        &script,
        format!(
            "import os, signal, time\n\
             signal.signal(signal.SIGHUP, signal.SIG_IGN)\n\
             n = 0\n\
             while n < 400:\n\
             \tn += 1\n\
             \tpid = os.fork()\n\
             \tif pid == 0:\n\
             \t\tos.execv('/bin/sleep', ['sleep', '{number}'])\n\
             \t# Keep forking through the kill window, then slow down.\n\
             \ttime.sleep(0.002 if n < 40 else 0.02)\n\
             time.sleep(30)\n"
        ),
    )
    .unwrap();
    d.ok(&format!("spawn loop python3 {}", script.display()));
    wait_until("fork loop is running", Duration::from_secs(8), || {
        pids_matching(&needle).len() >= 15
    });
    d.ok("kill loop");
    thread::sleep(Duration::from_millis(400));
    let left = pids_matching(&needle);
    let detail: Vec<String> = left
        .iter()
        .map(|pid| {
            let sid = fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|stat| {
                    let rest = &stat[stat.rfind(')')? + 1..];
                    Some(rest.split_whitespace().nth(3)?.to_string())
                })
                .unwrap_or_else(|| "?".into());
            format!("{pid} sid={sid}")
        })
        .collect();
    d.alive();
    assert!(
        left.is_empty(),
        "kill left {} process(es) whose cmdline matches {needle}: {detail:?}",
        left.len()
    );
}

/// A foreground process chooses its own comm. `info` and `settle` print that
/// string into a reply the CLI writes to the user's terminal.
#[test]
fn foreground_comm_cannot_inject_controls_into_info_or_settle() {
    let d = Daemon::start();
    let script = d.dir.join("fg.py");
    fs::write(
        &script,
        "import ctypes, time\nctypes.CDLL(None).prctl(15, b'\\x1b[2J\\x1b[H#')\ntime.sleep(60)\n",
    )
    .unwrap();
    d.bash("sh");
    d.ok("send sh set -m\\r");
    d.wait("sh", "$");
    d.ok(&format!("send sh python3 {}\\r", script.display()));

    let deadline = Instant::now() + Duration::from_secs(8);
    let mut comm = String::new();
    while Instant::now() < deadline {
        comm.clear();
        for pid in pids_matching("fg.py") {
            if let Ok(raw) = fs::read(format!("/proc/{pid}/comm")) {
                let text = String::from_utf8_lossy(&raw).into_owned();
                if text.contains('\u{1b}') {
                    comm = text;
                    break;
                }
            }
        }
        if !comm.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        comm.contains('\u{1b}'),
        "setup failed: python comm never contained ESC (last {comm:?})"
    );

    let mut info = String::new();
    let info_deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < info_deadline {
        info = d.ok("info sh");
        let fg = info
            .lines()
            .find_map(|l| l.strip_prefix("foreground\t"))
            .unwrap_or("");
        if fg != "-" && !fg.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let settle = d.ok("settle sh 400 40");
    assert!(
        !info.contains('\u{1b}') && !settle.contains('\u{1b}'),
        "control bytes from comm reached a reply\ncomm={comm:?}\ninfo={info:?}\nsettle={settle:?}"
    );
}

/// Past MAX_CLIENTS the accept loop still spawns one thread per connection
/// to deliver ERR busy. Those threads are not counted against the cap.
#[test]
fn busy_replies_stay_within_the_client_cap() {
    let d = Daemon::start();
    let _held: Vec<UnixStream> = (0..512)
        .map(|_| UnixStream::connect(&d.sock).unwrap())
        .collect();
    let probe = d.req("list");
    assert!(
        probe.starts_with("ERR busy"),
        "cap was not reached: {probe}"
    );
    let base = thread_count(d.pid());
    let mut extra = Vec::new();
    let mut samples = Vec::new();
    for _ in 0..40 {
        extra.push(UnixStream::connect(&d.sock).unwrap());
        samples.push(thread_count(d.pid()));
    }
    let peak = samples.iter().copied().max().unwrap_or(base);
    // Release the cap before touching the daemon again.
    drop(extra);
    drop(_held);
    let alive = wait_list_ok(&d);
    assert!(
        peak < base + 8,
        "threads grew past the cap: base {base}, peak {peak}, samples {samples:?}, list after release: {alive:?}"
    );
}

fn wait_list_ok(d: &Daemon) -> String {
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut last = String::new();
    while Instant::now() < deadline {
        last = d.req("list");
        if last.starts_with("OK") {
            return last;
        }
        thread::sleep(Duration::from_millis(20));
    }
    last
}

/// `screen` / `history` must not carry bytes a terminal would interpret,
/// including bidi format characters.
#[test]
fn screen_and_history_carry_no_terminal_controls() {
    let d = Daemon::start();
    let path = d.dir.join("paint.bin");
    let mut data = Vec::new();
    data.extend_from_slice(b"A");
    data.extend_from_slice("\u{061C}".as_bytes()); // Arabic letter mark
    data.extend_from_slice(b"B");
    data.extend_from_slice("\u{202E}".as_bytes()); // RLO
    data.extend_from_slice(b"C");
    data.extend_from_slice("\u{009B}".as_bytes()); // C1 CSI
    data.extend_from_slice(b"D");
    data.extend_from_slice(b"\x1b[31mE");
    data.extend_from_slice("\u{200F}".as_bytes()); // RLM
    data.extend_from_slice(b"\nEND\n");
    fs::write(&path, &data).unwrap();
    d.ok(&format!("spawn vis cat {}", path.display()));

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut screen = String::new();
    while Instant::now() < deadline {
        screen = d.screen("vis");
        if screen.contains("END") {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        screen.contains("END"),
        "cat output never arrived: {screen:?}"
    );
    let history = d.ok("history vis");
    let resize = d.req("resize vis 1 1");
    assert!(resize.starts_with("OK"), "{resize}");
    assert!(d.req("resize vis 40 120").starts_with("OK"));
    d.alive();

    let mut leaked = Vec::new();
    for (label, text) in [("screen", screen.as_str()), ("history", history.as_str())] {
        for (name, ch) in [
            ("ESC", '\u{1b}'),
            ("C1 CSI", '\u{9b}'),
            ("RLO", '\u{202e}'),
            ("RLM", '\u{200f}'),
            ("ALM", '\u{61c}'),
        ] {
            if text.chars().any(|c| c == ch) {
                leaked.push(format!("{label} contains {name}"));
            }
        }
        for c in text.chars() {
            if c != '\n' && c.is_control() {
                leaked.push(format!("{label} contains control U+{:04X}", c as u32));
            }
        }
    }
    assert!(
        leaked.is_empty(),
        "{leaked:?}\nscreen={screen:?}\nhistory={history:?}"
    );
}

/// `log` must not follow a symlink onto some other file.
#[test]
fn log_does_not_follow_a_symlink() {
    let d = Daemon::start();
    d.ok("spawn s cat");
    thread::sleep(Duration::from_millis(100));
    let target = d.dir.join("target");
    let link = d.dir.join("link");
    fs::write(&target, b"").unwrap();
    symlink(&target, &link).unwrap();
    let reply = d.req(&format!("log s {}", link.display()));
    d.ok("send s SECRET-AUDIT-MARKER\\n");
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut body = Vec::new();
    while Instant::now() < deadline {
        body = fs::read(&target).unwrap_or_default();
        if !body.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let leaked = body
        .windows(b"SECRET-AUDIT-MARKER".len())
        .any(|w| w == b"SECRET-AUDIT-MARKER");
    assert!(
        !leaked,
        "session output was written through the symlink\nlog reply={reply}\ntarget={body:?}"
    );
}

/// Killing a session must drop the PTY descriptors it held in the daemon.
#[test]
fn killing_a_session_releases_its_descriptors() {
    let d = Daemon::start();
    let before = fd_count(d.pid());
    d.ok("spawn held sleep 30");
    let mid = fd_count(d.pid());
    assert!(mid > before, "spawn opened no fds ({before} -> {mid})");
    d.ok("kill held");
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut after = mid;
    while Instant::now() < deadline {
        after = fd_count(d.pid());
        if after <= before + 1 {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        after <= before + 1,
        "fds stayed open after kill: before {before}, with session {mid}, after {after}"
    );
    d.alive();
}

/// Shuts the hand-rolled daemon down even when the assertion panics.
struct HandRolled {
    child: std::process::Child,
    dir: std::path::PathBuf,
    sock: std::path::PathBuf,
}

impl Drop for HandRolled {
    fn drop(&mut self) {
        if let Ok(mut s) = UnixStream::connect(&self.sock) {
            let _ = s.write_all(b"shutdown\n");
            let mut sink = String::new();
            let _ = s.read_to_string(&mut sink);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// A non-cloexec descriptor inherited from the daemon's parent must not be
/// visible inside a session. The test plants one on purpose, before `serve`:
/// a shell redirection leaves fd 7 open (and not close-on-exec) across exec.
#[test]
fn inherited_extra_fd_is_not_given_to_the_program() {
    let dir = private_dir();
    let secret = dir.join("secret");
    let sock = dir.join("s.sock");
    fs::write(&secret, b"planted").unwrap();
    let child = Command::new("sh")
        .arg("-c")
        .arg("exec 7<\"$1\"; exec \"$0\" serve")
        .arg(BIN)
        .arg(&secret)
        .env("XUMUX_SOCK", &sock)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until("daemon socket", Duration::from_secs(5), || {
        UnixStream::connect(&sock).is_ok()
    });
    let daemon = HandRolled { child, dir, sock };

    let mut stream = UnixStream::connect(&daemon.sock).unwrap();
    stream
        .write_all(b"spawn probe ls -l /proc/self/fd\n")
        .unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap();
    assert!(reply.starts_with("OK\n"), "{reply}");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut screen = String::new();
    while Instant::now() < deadline {
        let mut s = UnixStream::connect(&daemon.sock).unwrap();
        s.write_all(b"screen probe\n").unwrap();
        screen.clear();
        s.read_to_string(&mut screen).unwrap();
        if screen.contains("total") {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        screen.contains("total"),
        "ls output never arrived, so this probe is inconclusive:\n{screen}"
    );
    assert!(
        !screen.contains("secret"),
        "program saw the planted non-cloexec file\n{screen}"
    );
}

/// A line longer than 1 MiB is rejected whole, not cut down and dispatched.
#[test]
fn oversize_request_is_not_truncated_into_a_command() {
    let d = Daemon::start();
    let mut req = b"spawn sneaky true ".to_vec();
    req.resize((1 << 20) + 32, b'A');
    req.push(b'\n');
    req.extend_from_slice(b"spawn sneaky true\n");
    let mut stream = UnixStream::connect(&d.sock).unwrap();
    let mut writer = stream.try_clone().unwrap();
    let send = thread::spawn(move || {
        let _ = writer.write_all(&req);
    });
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap();
    let _ = send.join();
    assert!(reply.starts_with("ERR request exceeds"), "{reply}");
    assert_eq!(d.ok("list"), "");
    d.alive();
}
