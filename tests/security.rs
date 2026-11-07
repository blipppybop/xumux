//! Security tests: the socket, the protocol, process isolation, and the
//! emulator as an attack surface.

mod common;
use common::*;

use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::Command;
use std::time::{Duration, Instant};
use std::{fs, thread};

// ------------------------------------------------------------ socket & filesystem

#[test]
fn socket_is_owner_only() {
    let d = Daemon::start();
    assert_eq!(mode(&d.sock), 0o600);
}

#[test]
fn second_daemon_on_same_socket_is_refused() {
    let d = Daemon::start();
    let out = Command::new(BIN)
        .arg("serve")
        .env("XUMUX_SOCK", &d.sock)
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("already running"));
    d.alive();
}

#[test]
fn stale_socket_is_replaced() {
    let dir = private_dir();
    let sock = dir.join("s.sock");
    drop(UnixListener::bind(&sock).unwrap()); // leaves a dead socket file behind
    assert!(sock.exists());
    let d = Daemon::start_at(dir, sock);
    d.alive();
}

fn cli_with_env(vars: &[(&str, &str)], remove: &[&str]) -> std::process::Output {
    let mut c = Command::new(BIN);
    c.arg("list");
    for k in remove {
        c.env_remove(k);
    }
    for (k, v) in vars {
        c.env(k, v);
    }
    c.output().unwrap()
}

#[test]
fn world_writable_runtime_dir_is_refused() {
    let dir = private_dir();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o777)).unwrap();
    let out = cli_with_env(
        &[("XDG_RUNTIME_DIR", dir.to_str().unwrap())],
        &["XUMUX_SOCK"],
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && err.contains("refusing") && err.contains("777"),
        "{err}"
    );
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn group_readable_runtime_dir_is_refused() {
    let dir = private_dir();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o750)).unwrap();
    let out = cli_with_env(
        &[("XDG_RUNTIME_DIR", dir.to_str().unwrap())],
        &["XUMUX_SOCK"],
    );
    assert!(String::from_utf8_lossy(&out.stderr).contains("refusing"));
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn symlinked_runtime_dir_is_refused() {
    let real = private_dir();
    let link = real.with_extension("link");
    let _ = fs::remove_file(&link);
    symlink(&real, &link).unwrap();
    let out = cli_with_env(
        &[("XDG_RUNTIME_DIR", link.to_str().unwrap())],
        &["XUMUX_SOCK"],
    );
    assert!(String::from_utf8_lossy(&out.stderr).contains("symlink"));
    fs::remove_file(&link).unwrap();
    fs::remove_dir_all(&real).unwrap();
}

#[test]
fn foreign_owned_runtime_dir_is_refused() {
    // /proc is owned by root: a stand-in for "a directory someone else owns".
    let out = cli_with_env(&[("XDG_RUNTIME_DIR", "/proc")], &["XUMUX_SOCK"]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("refusing") && err.contains("uid 0"), "{err}");
}

#[test]
fn tmp_fallback_is_a_private_per_user_dir() {
    let out = cli_with_env(&[], &["XUMUX_SOCK", "XDG_RUNTIME_DIR"]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!err.contains("refusing"), "{err}");
    let uid = fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|l| {
            l.strip_prefix("Uid:")
                .map(|v| v.split_whitespace().next().unwrap().to_string())
        })
        .unwrap();
    let dir = std::path::PathBuf::from(format!("/tmp/xumux-{uid}"));
    assert_eq!(mode(&dir), 0o700);
}

// ------------------------------------------------------------ protocol abuse

#[test]
fn oversized_request_is_rejected_not_executed() {
    let d = Daemon::start();
    let mut s = UnixStream::connect(&d.sock).unwrap();
    let mut w = s.try_clone().unwrap();
    // "spawn" followed by 2 MiB: if it were truncated and executed, a session would appear.
    let writer = thread::spawn(move || {
        let mut big = b"spawn huge sh -c true ".to_vec();
        big.resize(2 << 20, b'a');
        big.push(b'\n');
        let _ = w.write_all(&big); // the daemon hangs up early; EPIPE is fine
    });
    let mut reply = String::new();
    s.read_to_string(&mut reply).unwrap();
    let _ = writer.join();
    assert!(reply.starts_with("ERR request exceeds"), "{reply}");
    assert_eq!(d.ok("list"), "");
}

#[test]
fn invalid_utf8_is_rejected() {
    let d = Daemon::start();
    assert!(
        d.raw(b"list\xff\xfe\n")
            .starts_with("ERR request is not valid UTF-8")
    );
    d.alive();
}

#[test]
fn incomplete_request_is_rejected() {
    let d = Daemon::start();
    let mut s = UnixStream::connect(&d.sock).unwrap();
    s.write_all(b"list").unwrap();
    s.shutdown(Shutdown::Write).unwrap();
    let mut reply = String::new();
    s.read_to_string(&mut reply).unwrap();
    assert!(reply.starts_with("ERR incomplete"), "{reply}");
}

#[test]
fn nul_bytes_and_control_chars_are_harmless() {
    let d = Daemon::start();
    for req in [
        &b"\0\n"[..],
        b"list\0\n",
        b"spawn \0 sh\n",
        b"\x1b[2J\n",
        b"kill \x07\n",
        b"\r\n",
        b"\n",
    ] {
        let r = d.raw(req);
        assert!(
            r.starts_with("OK\n") || r.starts_with("ERR "),
            "{req:?} -> {r:?}"
        );
    }
    d.alive();
}

#[test]
fn random_garbage_never_kills_the_daemon() {
    let d = Daemon::start();
    d.bash("victim");
    let words = [
        "spawn",
        "send",
        "keys",
        "screen",
        "history",
        "wait",
        "list",
        "info",
        "log",
        "resize",
        "rename",
        "kill",
        "victim",
        "0",
        "-1",
        "99999999999999999999",
        "1000",
        "1001",
        "\\x",
        "\\",
        "\\xZZ",
        "C-",
        "M-",
        "../..",
        "/etc/passwd",
        "off",
        "日本",
        "\u{202e}",
        "",
        " ",
    ];
    let mut rng = Rng(0xDEADBEEF);
    for _ in 0..1500 {
        let mut line = String::new();
        for _ in 0..rng.below(6) {
            if rng.below(5) == 0 {
                line.push(char::from_u32(rng.below(0x3000) as u32).unwrap_or('?'));
            } else {
                line += words[rng.below(words.len() as u64) as usize];
            }
            line.push(' ');
        }
        // Never ask for these by accident: they would end the test early.
        if line.starts_with("shutdown")
            || line.contains("kill victim")
            || line.contains("rename victim")
        {
            continue;
        }
        let line = line.replace('\n', " ");
        let r = d.raw(format!("{line}\n").as_bytes());
        assert!(
            r.starts_with("OK\n") || r.starts_with("ERR "),
            "{line:?} -> {r:?}"
        );
    }
    d.alive();
    assert!(d.ok("list").contains("victim\t"));
}

#[test]
fn idle_connections_time_out_and_do_not_block_others() {
    let d = Daemon::start();
    let idle: Vec<UnixStream> = (0..200)
        .map(|_| UnixStream::connect(&d.sock).unwrap())
        .collect();
    let t = Instant::now();
    d.alive();
    assert!(
        t.elapsed() < Duration::from_secs(1),
        "list took {:?} behind idle clients",
        t.elapsed()
    );
    let mut first = &idle[0];
    let mut reply = String::new();
    let t = Instant::now();
    first.read_to_string(&mut reply).unwrap();
    assert!(reply.starts_with("ERR timed out"), "{reply}");
    assert!(t.elapsed() < Duration::from_secs(8));
}

#[test]
fn slow_loris_byte_at_a_time_is_cut_off() {
    let d = Daemon::start();
    let mut s = UnixStream::connect(&d.sock).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let t = Instant::now();
    // Dribble a never-ending request; the 5 s timeout must still fire overall
    // or per read. Either way the daemon must answer and close.
    let mut w = s.try_clone().unwrap();
    let dribbler = thread::spawn(move || {
        for _ in 0..80 {
            if w.write_all(b"l").is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
    });
    let mut reply = String::new();
    let _ = s.read_to_string(&mut reply);
    let _ = dribbler.join();
    assert!(reply.starts_with("ERR"), "{reply:?}");
    assert!(t.elapsed() < Duration::from_secs(10));
    d.alive();
}

#[test]
fn session_names_cannot_inject() {
    let d = Daemon::start();
    for bad in ["../x", "a/b", "a\tb", "$(id)", "a;b", "é", &"x".repeat(65)] {
        let msg = d.err(&format!("spawn {bad} true"));
        assert!(msg.contains("invalid name"), "{bad}: {msg}");
    }
    assert!(d.err("rename nope ../x").contains("invalid name"));
    assert_eq!(d.ok("list"), "");
}

#[test]
fn log_requires_absolute_path_and_creates_private_file() {
    let d = Daemon::start();
    d.bash("s");
    assert!(d.err("log s relative.log").contains("absolute"));
    let path = d.dir.join("out.log");
    d.ok(&format!("log s {}", path.display()));
    assert_eq!(mode(&path), 0o600);
}

#[test]
fn bad_sizes_and_counts_are_rejected() {
    let d = Daemon::start();
    d.bash("s");
    for bad in ["0 10", "10 0", "1001 10", "10 99999", "-1 5", "a b", "10"] {
        let r = d.req(&format!("resize s {bad}"));
        assert!(r.starts_with("ERR"), "{bad}: {r}");
    }
    assert!(d.err("history s -5").contains("bad line count"));
    assert!(d.err("wait s abc x").contains("usage"));
}

// ------------------------------------------------------------ process isolation

#[test]
fn no_file_descriptors_leak_into_programs() {
    let d = Daemon::start();
    d.bash("other"); // a second PTY that must stay invisible
    d.ok(&format!("log other {}", d.dir.join("other.log").display()));
    d.ok("spawn probe ls -l /proc/self/fd");
    d.wait("probe", "total");
    let screen = d.screen("probe");
    let targets: Vec<&str> = screen
        .lines()
        .filter_map(|l| l.split(" -> ").nth(1))
        .collect();
    assert!(!targets.is_empty(), "{screen}");
    for t in &targets {
        assert!(
            t.starts_with("/dev/pts/") || t.starts_with("/proc/"),
            "leaked fd -> {t}\n{screen}"
        );
    }
    // exactly stdin/stdout/stderr on one pty, plus ls's own directory handle
    let ptys: Vec<_> = targets
        .iter()
        .filter(|t| t.starts_with("/dev/pts/"))
        .collect();
    assert_eq!(ptys.len(), 3, "{screen}");
    assert!(ptys.iter().all(|p| p == &ptys[0]));
}

#[test]
fn each_program_gets_its_own_session_and_controlling_tty() {
    let d = Daemon::start();
    for name in ["a", "b"] {
        d.ok(&format!(
            "spawn {name} sh -c 'echo \"sid=$(cut -d\" \" -f6 /proc/$$/stat) pid=$$ tty=$(tty)\"; sleep 30'"
        ));
        d.wait(name, "tty=");
    }
    let parse = |name: &str| {
        let s = d.screen(name);
        let line = s
            .lines()
            .find(|l| l.starts_with("sid="))
            .unwrap()
            .to_string();
        let f: Vec<&str> = line.split(' ').collect();
        (
            f[0][4..].to_string(),
            f[1][4..].to_string(),
            f[2][4..].to_string(),
        )
    };
    let (sid_a, pid_a, tty_a) = parse("a");
    let (_, _, tty_b) = parse("b");
    assert_eq!(sid_a, pid_a, "program must lead its own session");
    assert!(tty_a.starts_with("/dev/pts/"));
    assert_ne!(tty_a, tty_b, "sessions must not share a terminal");
}

#[test]
fn kill_takes_down_background_jobs_and_orphans() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s sleep 7771001 & sleep 7771002 & disown -a; nohup sleep 7771003 >/dev/null 2>&1 &\\r");
    wait_until("sleeps started", Duration::from_secs(15), || {
        pids_matching("sleep 777100").len() == 3
    });
    // An orphan: its parent exits immediately, so it is reparented away from
    // the session tree but keeps the session id.
    // nohup: without it the kernel's hangup on session-leader exit kills it first.
    d.ok("spawn orphan sh -c 'nohup sleep 7771004 >/dev/null 2>&1 & sleep 0.2; exit 0'");
    wait_until("orphan started", Duration::from_secs(15), || {
        !pids_matching("sleep 7771004").is_empty()
    });
    d.ok("kill s");
    d.ok("kill orphan");
    wait_until("all sleeps gone", Duration::from_secs(15), || {
        pids_matching("sleep 777100").is_empty()
    });
}

#[test]
fn killing_a_session_does_not_touch_others() {
    let d = Daemon::start();
    let a = d.bash("a");
    let b = d.bash("b");
    d.ok("kill a");
    wait_until("a gone", Duration::from_secs(5), || !proc_alive(a));
    assert!(proc_alive(b));
    d.ok("send b echo $((20+22))\\r");
    d.wait("b", "42");
}

// ------------------------------------------------------------ hostile programs

#[test]
fn screen_output_cannot_carry_escape_injection() {
    let d = Daemon::start();
    // C1 CSI (U+009B), RTL override, isolate, OSC 52 clipboard write, DCS, raw ESC, BEL, DEL
    d.ok(r"spawn evil printf 'A\302\23331mB\342\200\256C\342\201\246D\033]52;c;aGVsbG8=\007E\033P+q\033\\F\033[6n\007\177G\nEND\n'");
    d.wait("evil", "END");
    for text in [d.screen("evil"), d.ok("history evil")] {
        assert!(text.contains("A31mBCDEFG"), "{text:?}");
        for c in text.chars() {
            assert!(
                c == '\n' || !c.is_control(),
                "control char {:?} in {text:?}",
                c
            );
            assert!(
                !matches!(c as u32, 0x202A..=0x202E | 0x2066..=0x2069),
                "bidi char in {text:?}"
            );
        }
    }
}

#[test]
fn hostile_title_is_sanitized() {
    let d = Daemon::start();
    d.ok(r"spawn t printf '\033]0;x\302\233y\342\200\256z\007\nEND\n'");
    d.wait("t", "END");
    assert_eq!(d.info("t", "title"), "xyz");
}

#[test]
fn query_flood_cannot_deadlock_a_session() {
    // Raw mode + thousands of cursor-position queries + never reading input:
    // the replies pile up. The session must keep running and `send` must not hang.
    let d = Daemon::start();
    d.ok(r#"spawn flood sh -c 'stty raw -echo; i=0; while [ $i -lt 20000 ]; do printf "\033[6n"; i=$((i+1)); done; echo FLOOD-DONE; sleep 30'"#);
    d.ok("wait flood 20000 FLOOD-DONE");
    let t = Instant::now();
    let r = d.req("send flood x");
    assert!(t.elapsed() < Duration::from_secs(1));
    assert!(r.starts_with("OK") || r.contains("input queue full"), "{r}");
    let before = (fd_count(d.pid()), thread_count(d.pid()));
    d.ok("kill flood");
    wait_until("flood threads gone", Duration::from_secs(5), || {
        fd_count(d.pid()) < before.0 && thread_count(d.pid()) < before.1
    });
}

#[test]
fn output_flood_keeps_the_daemon_responsive() {
    let d = Daemon::start();
    d.ok("spawn flood sh -c 'head -c 30000000 /dev/zero | tr \"\\0\" x; echo; echo FLOOD-DONE; sleep 30'");
    let mut worst = Duration::ZERO;
    let start = Instant::now();
    while !d.screen("flood").contains("FLOOD-DONE") {
        let t = Instant::now();
        d.ok("list");
        d.ok("info flood");
        worst = worst.max(t.elapsed());
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "flood never finished"
        );
    }
    assert!(
        worst < Duration::from_millis(250),
        "worst latency during flood: {worst:?}"
    );
}

#[test]
fn huge_input_is_bounded() {
    let d = Daemon::start();
    d.ok("spawn sink sh -c 'stty raw -echo; sleep 30'"); // never reads its input
    thread::sleep(Duration::from_millis(200));
    let chunk = "y".repeat(60_000);
    let mut full = false;
    for _ in 0..2000 {
        let r = d.req(&format!("send sink {chunk}"));
        if r.contains("input queue full") {
            full = true;
            break;
        }
        assert!(r.starts_with("OK"), "{r}");
    }
    assert!(
        full,
        "input queue never filled: memory would grow without bound"
    );
    d.alive();
}

#[test]
fn programs_cannot_crash_the_emulator() {
    // Random bytes straight from the kernel into the emulator.
    let d = Daemon::start();
    // CAN + ST + RIS afterwards: random bytes may leave an unterminated string
    // or escape open (real terminals would swallow the marker too).
    d.ok(r#"spawn noise sh -c 'head -c 3000000 /dev/urandom; printf "\030\033\\\\\033c"; echo NOISE-DONE; sleep 30'"#);
    d.ok("wait noise 30000 NOISE-DONE");
    d.ok("resize noise 7 13");
    d.ok("resize noise 1000 1000");
    d.ok("history noise 50");
    d.alive();
}

#[test]
fn log_to_fifo_or_device_is_refused_without_hanging() {
    let d = Daemon::start();
    d.bash("s");
    let fifo = d.dir.join("fifo");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let t = Instant::now();
    let msg = d.err(&format!("log s {}", fifo.display()));
    assert!(t.elapsed() < Duration::from_secs(1), "log open hung");
    assert!(
        msg.contains("No such device") || msg.contains("not a regular file"),
        "{msg}"
    );
    assert!(d.err("log s /dev/null").contains("not a regular file"));
    d.ok("send s echo $((5+5))\\r");
    d.wait("s", "10");
}

#[test]
fn stalled_attached_client_cannot_slow_the_session() {
    let d = Daemon::start();
    d.bash("s");
    // Attach over a raw socket and never read a byte.
    let mut stalled = UnixStream::connect(&d.sock).unwrap();
    stalled.write_all(b"attach s 0 0\n").unwrap();
    wait_until("attached", Duration::from_secs(5), || {
        d.info("s", "attached") == "1"
    });
    let t = Instant::now();
    // ~20 MB: far more than a client may lag behind (1024 chunks), so the
    // stalled client must be dropped rather than buffered without bound.
    d.ok("send s seq 1 3000000; echo DONE-$((1+1))\\r");
    d.ok("wait s 120000 DONE-2");
    assert!(
        t.elapsed() < Duration::from_secs(60),
        "session slowed to {:?}",
        t.elapsed()
    );
    wait_until("stalled client dropped", Duration::from_secs(10), || {
        d.info("s", "attached") == "0"
    });
    drop(stalled);
}

#[test]
fn explicit_socket_path_must_also_be_in_a_private_dir() {
    // /tmp is world-writable: a socket there could be reached or swapped.
    for path in ["/tmp/xumux-shared.sock", "/proc/x.sock"] {
        let out = Command::new(BIN)
            .arg("serve")
            .env("XUMUX_SOCK", path)
            .output()
            .unwrap();
        assert!(!out.status.success());
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("refusing socket dir"),
            "{path}"
        );
    }
    assert!(!std::path::Path::new("/tmp/xumux-shared.sock").exists());
}
