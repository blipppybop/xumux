//! Every command, through the socket and through the real CLI.

mod common;
use common::*;

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use std::{fs, thread};

fn lines_equal(screen: &str, text: &str) -> usize {
    screen.lines().filter(|l| l.trim_end() == text).count()
}

// ------------------------------------------------------------ CLI basics

#[test]
fn help_and_unknown_commands() {
    let d = Daemon::start();
    let out = Command::new(BIN).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage: xumux"));
    let out = d.cli(&["frobnicate"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown command 'frobnicate'"));
}

#[test]
fn cli_reports_missing_daemon() {
    let dir = private_dir();
    let out = Command::new(BIN)
        .arg("list")
        .env("XUMUX_SOCK", dir.join("nobody-here.sock"))
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("is `xumux serve` running?"));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn cli_round_trip_with_quoting_and_newlines() {
    let d = Daemon::start();
    d.bash("s");
    // A literal newline in an argument must arrive as one request, not two.
    let out = d.cli(&["send", "s", "echo $((6*7))\n"]);
    assert!(out.status.success(), "{out:?}");
    d.wait("s", "42");
    let out = d.cli(&["screen", "s"]);
    assert!(String::from_utf8_lossy(&out.stdout).contains("42"));
}

// ------------------------------------------------------------ spawn / list / info / rename / kill

#[test]
fn spawn_list_info() {
    let d = Daemon::start();
    let pid = d.bash("one");
    assert!(pid > 1);
    d.bash("two");
    let list = d.ok("list");
    let rows: Vec<Vec<&str>> = list.lines().map(|l| l.split('\t').collect()).collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "one", "list is sorted");
    assert_eq!(rows[0][1], pid.to_string());
    assert_eq!(rows[0][2], "running");
    assert!(rows[0][3].contains("bash"));
    assert_eq!(d.info("one", "pid"), pid.to_string());
    assert_eq!(d.info("one", "size"), "40x120");
    assert_eq!(d.info("one", "alt_screen"), "no");
    assert_eq!(d.info("one", "attached"), "0");
    assert_eq!(d.info("one", "log"), "-");
}

#[test]
fn spawn_errors() {
    let d = Daemon::start();
    d.bash("dup");
    assert!(
        d.err(&format!("spawn dup {BASH}"))
            .contains("already exists")
    );
    assert!(d.err("spawn").contains("usage"));
    assert!(d.err("spawn lonely").contains("usage"));
    assert!(d.err("spawn x    ").contains("usage"));
}

#[test]
fn spawn_command_not_found_reports_exit_127() {
    let d = Daemon::start();
    d.ok("spawn nope definitely-not-a-real-command-xyz");
    wait_until("exit", Duration::from_secs(5), || {
        d.ok("list").contains("exited(127)")
    });
    assert!(d.screen("nope").contains("not found"));
}

#[test]
fn exit_codes_and_signals_are_reported() {
    let d = Daemon::start();
    d.ok("spawn seven sh -c 'exit 7'");
    d.ok("spawn zero true");
    let pid: u32 = d.ok("spawn victim sleep 60").trim().parse().unwrap();
    unsafe_free_kill(pid, "TERM");
    wait_until("statuses", Duration::from_secs(5), || {
        let l = d.ok("list");
        l.contains("seven\t")
            && l.contains("exited(7)")
            && l.contains("exited(0)")
            && l.contains("killed(sig 15)")
    });
}

fn unsafe_free_kill(pid: u32, sig: &str) {
    assert!(
        Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(pid.to_string())
            .status()
            .unwrap()
            .success()
    );
}

#[test]
fn spawn_working_dir_and_environment() {
    let d = Daemon::start();
    let dir = d.dir.join("work");
    fs::create_dir(&dir).unwrap();
    d.ok(&format!(
        "spawn env sh -c 'cd {} && export FOO=bar && echo cwd=$(pwd) foo=$FOO term=$TERM; sleep 5'",
        dir.display()
    ));
    d.wait("env", "term=");
    let s = d.screen("env");
    assert!(s.contains(&format!("cwd={}", dir.display())), "{s}");
    assert!(
        s.contains("foo=bar") && s.contains("term=xterm-256color"),
        "{s}"
    );
    // default cwd is $HOME
    d.ok("spawn home pwd");
    d.wait("home", "/");
    assert_eq!(d.screen("home").trim(), std::env::var("HOME").unwrap());
}

#[test]
fn rename() {
    let d = Daemon::start();
    d.bash("a");
    d.bash("b");
    assert!(d.err("rename a b").contains("already exists"));
    assert!(d.err("rename nope c").contains("no session"));
    assert!(d.err("rename a").contains("usage"));
    d.ok("rename a c");
    let list = d.ok("list");
    assert!(list.contains("c\t") && !list.contains("a\t"));
    d.ok("send c echo $((11*11))\\r");
    d.wait("c", "121");
}

#[test]
fn kill_and_reuse_name() {
    let d = Daemon::start();
    let pid = d.bash("s");
    d.ok("kill s");
    assert!(d.err("kill s").contains("no session"));
    wait_until("dead", Duration::from_secs(5), || !proc_alive(pid));
    let pid2 = d.bash("s");
    assert_ne!(pid, pid2);
}

#[test]
fn kill_already_exited_session() {
    let d = Daemon::start();
    d.ok("spawn quick true");
    wait_until("exit", Duration::from_secs(5), || {
        d.ok("list").contains("exited(0)")
    });
    d.ok("kill quick");
    assert_eq!(d.ok("list"), "");
}

// ------------------------------------------------------------ input: send / keys

#[test]
fn send_escapes() {
    let d = Daemon::start();
    d.bash("s");
    d.ok(r"send s printf '[%s]\\n' \x41\x42\tC\r");
    d.wait("s", "[AB");
    assert!(d.screen("s").contains("[AB\tC]") || d.screen("s").contains("[AB"));
    assert!(d.err(r"send s \q").contains("bad escape"));
    assert!(d.err(r"send s \x4").contains("bad \\x"));
    assert!(d.err("send nobody hi").contains("no session"));
}

#[test]
fn send_preserves_spacing() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s echo \"a   b    c\"\\r");
    d.wait("s", "a   b    c");
}

#[test]
fn keys_enter_and_literal_text() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("keys s echo $((40+2)) Enter");
    d.wait("s", "42");
}

#[test]
fn keys_ctrl_c_interrupts() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("keys s sleep 1000 Enter");
    thread::sleep(Duration::from_millis(300));
    d.ok("keys s C-c");
    d.ok("keys s echo $((50+5)) Enter");
    d.wait("s", "55");
}

#[test]
fn keys_arrows_recall_history() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("keys s echo $((300+33)) Enter");
    d.wait("s", "333");
    d.ok("keys s Up Enter");
    wait_until("second 333", Duration::from_secs(5), || {
        lines_equal(&d.screen("s"), "333") == 2
    });
}

#[test]
fn keys_backspace_left_and_ctrl_a() {
    let d = Daemon::start();
    d.bash("s");
    // type "echo 12x", erase x, add 3 -> 123
    d.ok("keys s echo 12x BSpace 3 Enter");
    d.wait("s", "123");
    // Ctrl-A jumps to line start: type "cho 9", C-a, "e" -> "echo 9"
    d.ok("keys s cho $((9*1)) C-a e Enter");
    wait_until("9", Duration::from_secs(5), || {
        lines_equal(&d.screen("s"), "9") == 1
    });
}

#[test]
fn keys_meta_and_function_keys_are_encoded() {
    // Read raw bytes back with od so we see exactly what was sent.
    let d = Daemon::start();
    d.ok("spawn raw sh -c 'stty raw -echo; head -c 16 | od -An -tx1; sleep 5'");
    thread::sleep(Duration::from_millis(300));
    d.ok("keys raw M-x F1 F5 Home C-@ Tab"); // 2 + 3 + 5 + 3 + 1 + 1 = 15 bytes
    d.ok("keys raw z"); // 16th byte
    d.wait("raw", "7a");
    let s = d.screen("raw").replace('\n', " ");
    assert!(
        s.contains("1b 78 1b 4f 50 1b 5b 31 35 7e 1b 5b 48 00 09 7a"),
        "{s}"
    );
}

// ------------------------------------------------------------ output: screen / history / wait / log

#[test]
fn history_keeps_scrollback() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s seq 1 500; echo SEQ-$((1+1))\\r");
    d.wait("s", "SEQ-2");
    let all = d.ok("history s");
    let nums: Vec<u32> = all.lines().filter_map(|l| l.parse().ok()).collect();
    assert_eq!(nums, (1..=500).collect::<Vec<_>>());
    let last = d.ok("history s 3");
    assert_eq!(last.lines().count(), 3, "{last}");
    assert!(d.info("s", "history").parse::<usize>().unwrap() >= 460);
}

#[test]
fn history_is_capped() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s seq 1 15000; echo SEQ-$((1+1))\\r");
    d.ok("wait s 30000 SEQ-2");
    assert_eq!(d.info("s", "history"), "10000");
    let first: u32 = d
        .ok("history s")
        .lines()
        .find_map(|l| l.parse().ok())
        .unwrap();
    assert!(first > 4000, "oldest kept line {first}");
}

#[test]
fn alt_screen_programs_do_not_pollute_history() {
    let d = Daemon::start();
    d.bash("s");
    d.ok(r"send s printf '\\033[?1049h'; seq 1 300; printf '\\033[?1049l'; echo ALT-$((1+1))\r");
    d.wait("s", "ALT-2");
    assert!(
        !d.ok("history s").lines().any(|l| l == "150"),
        "alt-screen output leaked into history"
    );
}

#[test]
fn wait_success_timeout_and_exit() {
    let d = Daemon::start();
    d.bash("s");
    let t = Instant::now();
    let msg = d.err("wait s 300 never-going-to-appear");
    assert!(msg.contains("timed out after 300 ms"), "{msg}");
    assert!(t.elapsed() >= Duration::from_millis(300) && t.elapsed() < Duration::from_secs(2));
    d.ok("spawn brief sh -c 'sleep 0.2; exit 3'");
    assert!(d.err("wait brief 5000 nope").contains("session exited"));
    d.ok("send s sleep 0.3; echo LATE-$((1+1))\\r");
    d.ok("wait s 5000 LATE-2");
    assert!(d.err("wait s").contains("usage"));
    assert!(d.err("wait s 100").contains("usage"));
}

#[test]
fn log_captures_raw_output_and_can_stop() {
    let d = Daemon::start();
    d.bash("s");
    let path = d.dir.join("session.log");
    d.ok(&format!("log s {}", path.display()));
    assert_eq!(d.info("s", "log"), path.display().to_string());
    d.ok(r"send s printf '\\033[1;31m%s\\033[0m\\n' RED\r");
    wait_until("red", Duration::from_secs(5), || {
        lines_equal(&d.screen("s"), "RED") == 1
    });
    wait_until("log written", Duration::from_secs(5), || {
        fs::read(&path)
            .map(|b| b.windows(10).any(|w| w == b"\x1b[1;31mRED"))
            .unwrap_or(false)
    });
    d.ok("log s off");
    assert_eq!(d.info("s", "log"), "-");
    let size = fs::metadata(&path).unwrap().len();
    d.ok("send s echo AFTER-$((1+1))\\r");
    d.wait("s", "AFTER-2");
    thread::sleep(Duration::from_millis(100));
    assert_eq!(fs::metadata(&path).unwrap().len(), size);
    assert!(
        d.err("log s /nonexistent-dir/x.log")
            .contains("No such file")
    );
}

#[test]
fn cli_log_resolves_relative_paths() {
    let d = Daemon::start();
    d.bash("s");
    let out = Command::new(BIN)
        .args(["log", "s", "rel.log"])
        .env("XUMUX_SOCK", &d.sock)
        .current_dir(&d.dir)
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    assert_eq!(
        d.info("s", "log"),
        d.dir.join("rel.log").display().to_string()
    );
}

#[test]
fn title_and_alt_screen_in_info() {
    let d = Daemon::start();
    d.bash("s");
    d.ok(r"send s printf '\\033]2;build: 42%%\\007\\033[?1049h'; sleep 30\r");
    wait_until("alt", Duration::from_secs(5), || {
        d.info("s", "alt_screen") == "yes"
    });
    assert_eq!(d.info("s", "title"), "build: 42%");
}

#[test]
fn unicode_round_trip() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s echo 'héllo 日本語 🚀 ok'\\r");
    wait_until("unicode", Duration::from_secs(5), || {
        lines_equal(&d.screen("s"), "héllo 日本語 🚀 ok") == 1
    });
}

// ------------------------------------------------------------ terminal: resize / stats / TUI

#[test]
fn resize_reaches_the_program() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("resize s 24 80");
    assert_eq!(d.info("s", "size"), "24x80");
    d.ok("send s stty size\\r");
    d.wait("s", "24 80");
    assert!(d.err("resize nope 10 10").contains("no session"));
}

#[test]
fn full_screen_tui_and_resize_redraw() {
    let d = Daemon::start();
    d.ok("spawn top top -d 0.2");
    d.wait("top", "load average");
    d.ok("resize top 15 100");
    wait_until(
        "top redraws at the new size",
        Duration::from_secs(5),
        || {
            let s = d.screen("top");
            s.lines().count() <= 15 && s.contains("load average")
        },
    );
    d.ok("keys top q");
    wait_until("top quit", Duration::from_secs(5), || {
        d.ok("list").contains("exited(0)")
    });
}

#[test]
fn stats_measures_cpu_and_memory() {
    let d = Daemon::start();
    d.ok("spawn burn sh -c 'while :; do :; done'");
    thread::sleep(Duration::from_millis(200));
    let body = d.ok("stats burn");
    let get = |k: &str| {
        body.lines()
            .find_map(|l| l.strip_prefix(&format!("{k}\t")))
            .unwrap()
            .to_string()
    };
    let cpu: f64 = get("cpu").trim_end_matches('%').parse().unwrap();
    assert!(cpu > 50.0, "{body}");
    assert!(get("rss").ends_with("MiB"));
    assert!(get("procs").parse::<u32>().unwrap() >= 1);
    d.ok("spawn idle sleep 30");
    let idle = d.ok("stats idle");
    assert!(idle.contains("cpu\t0.0%"), "{idle}");
}

#[test]
fn stats_counts_the_whole_tree() {
    let d = Daemon::start();
    d.ok("spawn tree sh -c 'sleep 30 & sleep 30 & sleep 30 & wait'");
    wait_until("children", Duration::from_secs(5), || {
        d.ok("stats tree").contains("procs\t4")
    });
}

// ------------------------------------------------------------ attach

fn attach(d: &Daemon, name: &str) -> std::process::Child {
    Command::new(BIN)
        .args(["attach", name])
        .env("XUMUX_SOCK", &d.sock)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn collect(child: std::process::Child) -> String {
    let out = child.wait_with_output().unwrap();
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn attach_types_sees_output_and_detaches() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s echo BEFORE-$((1+1))\\r");
    d.wait("s", "BEFORE-2");
    let mut c = attach(&d, "s");
    wait_until("attached", Duration::from_secs(5), || {
        d.info("s", "attached") == "1"
    });
    c.stdin
        .as_mut()
        .unwrap()
        .write_all(b"echo $((7*111))\r")
        .unwrap();
    d.wait("s", "777");
    c.stdin.as_mut().unwrap().write_all(&[0x1d]).unwrap(); // Ctrl-]
    let out = collect(c);
    assert!(out.contains("BEFORE-2"), "initial redraw missing: {out:?}");
    assert!(out.contains("777"), "live output missing: {out:?}");
    assert!(out.contains("[xumux: detached]"));
    wait_until("detached", Duration::from_secs(5), || {
        d.info("s", "attached") == "0"
    });
    d.alive();
    assert!(
        d.ok("list").contains("running"),
        "detaching must not kill the session"
    );
}

#[test]
fn attach_many_clients_see_the_same_output() {
    let d = Daemon::start();
    d.bash("s");
    let clients: Vec<_> = (0..10).map(|_| attach(&d, "s")).collect();
    wait_until("10 attached", Duration::from_secs(5), || {
        d.info("s", "attached") == "10"
    });
    d.ok("send s echo BROADCAST-$((1000+1))\\r");
    d.wait("s", "BROADCAST-1001");
    thread::sleep(Duration::from_millis(200));
    for mut c in clients {
        c.stdin.as_mut().unwrap().write_all(&[0x1d]).unwrap();
        assert!(collect(c).contains("BROADCAST-1001"));
    }
}

#[test]
fn attached_client_exits_when_session_ends() {
    let d = Daemon::start();
    d.ok("spawn brief sh -c 'sleep 0.5; echo BYE'");
    let c = attach(&d, "brief");
    let t = Instant::now();
    let out = collect(c);
    assert!(t.elapsed() < Duration::from_secs(5));
    assert!(out.contains("BYE"), "{out:?}");
}

#[test]
fn attach_with_piped_input_keeps_streaming_until_exit() {
    let d = Daemon::start();
    d.bash("s");
    let mut c = attach(&d, "s");
    c.stdin
        .take()
        .unwrap()
        .write_all(b"echo PIPED-$((2+3)); sleep 0.3; exit\r")
        .unwrap(); // stdin closes
    let out = collect(c);
    assert!(out.contains("PIPED-5"), "{out:?}");
}

#[test]
fn attach_errors() {
    let d = Daemon::start();
    let out = d.cli(&["attach", "ghost"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("no session 'ghost'"));
    let out = d.cli(&["attach"]);
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage"));
}

#[test]
fn attach_over_raw_socket_resizes_and_streams() {
    let d = Daemon::start();
    d.bash("s");
    let mut sock = UnixStream::connect(&d.sock).unwrap();
    // keystrokes in the same packet as the request must not be lost
    sock.write_all(b"attach s 30 90\necho $((123+321))\r")
        .unwrap();
    d.wait("s", "444");
    assert_eq!(d.info("s", "size"), "30x90");
    sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut buf = vec![0u8; 65536];
    let n = sock.read(&mut buf).unwrap();
    assert!(buf[..n].starts_with(b"OK\n"));
}

// ------------------------------------------------------------ shutdown

#[test]
fn shutdown_kills_everything_and_removes_socket() {
    let d = Daemon::start();
    let pids: Vec<u32> = (0..5).map(|i| d.bash(&format!("s{i}"))).collect();
    let body = d.ok("shutdown");
    assert_eq!(body, "killed 5 sessions\n");
    wait_until("all dead", Duration::from_secs(5), || {
        pids.iter().all(|&p| !proc_alive(p))
    });
    wait_until("socket gone", Duration::from_secs(5), || !d.sock.exists());
}

// ------------------------------------------------------------ agent commands

#[test]
fn run_returns_only_new_output_and_keeps_state() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s seq 1 100\\r"); // old output that must not come back
    d.wait("s", "100");
    d.ok("settle s");
    let out = d.ok("run s 5000 cd / && echo here-$((2*21))");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[0], "$ cd / && echo here-$((2*21))", "{out}");
    assert_eq!(lines[1], "here-42");
    assert_eq!(lines.last(), Some(&"[xumux] quiet"));
    assert!(!out.contains("\n50\n"), "old output leaked into run: {out}");
    let out = d.ok("run s 5000 pwd");
    assert!(out.lines().any(|l| l == "/"), "cd did not persist: {out}");
}

#[test]
fn run_waits_for_silent_commands_in_a_shell() {
    let d = Daemon::start();
    d.bash("s");
    let t = Instant::now();
    let out = d.ok("run s 10000 sleep 1; echo woke-$((1+1))");
    assert!(
        t.elapsed() >= Duration::from_secs(1),
        "returned while sleep was running"
    );
    assert!(
        out.contains("\nwoke-2\n") && out.ends_with("[xumux] quiet\n"),
        "{out}"
    );
}

#[test]
fn run_timeout_names_the_running_command() {
    let d = Daemon::start();
    d.bash("s");
    let out = d.ok("run s 400 sleep 30");
    assert!(
        out.ends_with("[xumux] timeout: sleep still running\n"),
        "{out}"
    );
    assert!(d.info("s", "foreground").starts_with("sleep ("));
    d.ok("keys s C-c");
    assert_eq!(d.ok("settle s 5000"), "quiet\n");
    assert_eq!(d.info("s", "foreground"), "-");
}

#[test]
fn run_reports_program_exit() {
    let d = Daemon::start();
    d.bash("s");
    let out = d.ok("run s 5000 exit 3");
    assert!(out.ends_with("[xumux] exited(3)\n"), "{out}");
}

#[test]
fn run_in_a_repl_and_full_screen_programs() {
    let d = Daemon::start();
    d.ok("spawn py python3 -q");
    d.ok("settle py 10000");
    let out = d.ok("run py 5000 6*7");
    assert!(out.lines().any(|l| l == "42"), "{out}");
    d.ok("spawn top top -d 0.5");
    d.wait("top", "load average");
    let out = d.ok("run top 3000 ");
    assert!(
        out.contains("load average") && out.lines().count() > 10,
        "full screen expected: {out}"
    );
}

#[test]
fn run_handles_scrolling_past_the_screen() {
    let d = Daemon::start();
    d.bash("s");
    let out = d.ok("run s 10000 seq 1 300");
    let nums: Vec<u32> = out.lines().filter_map(|l| l.parse().ok()).collect();
    assert_eq!(nums, (1..=300).collect::<Vec<_>>());
}

#[test]
fn settle_waits_for_output_to_stop() {
    let d = Daemon::start();
    d.ok("spawn ticker sh -c 'for i in 1 2 3 4 5; do echo tick$i; sleep 0.1; done; sleep 30'");
    assert_eq!(d.ok("settle ticker 5000"), "quiet\n");
    assert!(d.screen("ticker").contains("tick5"));
    d.ok("spawn noisy sh -c 'while :; do echo x; sleep 0.05; done'");
    assert_eq!(d.ok("settle noisy 400"), "timeout\n");
    assert!(d.err("settle noisy abc").contains("usage"));
}

#[test]
fn wait_alternatives_report_the_match() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s echo build-$((0+1))-failed\\r");
    assert_eq!(
        d.ok("wait s 5000 ok-1 || build-1-failed || other"),
        "build-1-failed\n"
    );
    assert!(d.err("wait s 200 nope || never").contains("timed out"));
}

#[test]
fn wait_exit_reports_code_signal_and_timeout() {
    let d = Daemon::start();
    d.ok("spawn a sh -c 'sleep 0.2; exit 9'");
    assert_eq!(d.ok("wait-exit a 5000"), "exited(9)\n");
    let pid: u32 = d.ok("spawn b sleep 30").trim().parse().unwrap();
    assert!(d.err("wait-exit b 200").contains("still running"));
    assert!(
        Command::new("kill")
            .arg("-INT")
            .arg(pid.to_string())
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(d.ok("wait-exit b 5000"), "killed(sig 2)\n");
    assert!(d.err("wait-exit b").contains("usage"));
}

#[test]
fn guide_works_with_and_without_a_daemon() {
    let out = Command::new(BIN)
        .arg("guide")
        .env("XUMUX_SOCK", "/nonexistent/x.sock")
        .output()
        .unwrap();
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.starts_with("# xumux: guide for AI agents")
            && text.contains("## 3. Command reference")
    );
    let d = Daemon::start();
    assert_eq!(d.ok("guide"), text);
}

// ------------------------------------------------------------ robustness

#[test]
fn client_survives_a_closed_stdout_pipe() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s seq 1 2000\\r");
    d.wait("s", "2000");
    for cmd in ["guide", "history s", "screen s"] {
        let out = Command::new("sh")
            .arg("-c")
            .arg(format!("{BIN} {cmd} | head -c 3; echo \"rc=$?\""))
            .env("XUMUX_SOCK", &d.sock)
            .output()
            .unwrap();
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            !err.contains("panicked") && !err.contains("Broken pipe"),
            "{cmd}: {err}"
        );
        assert!(String::from_utf8_lossy(&out.stdout).ends_with("rc=0\n"));
    }
}

#[test]
fn daemon_starts_with_stderr_closed() {
    let dir = private_dir();
    let sock = dir.join("s.sock");
    let mut c = Command::new("sh")
        .arg("-c")
        .arg(format!("exec {BIN} serve 2>&-"))
        .env("XUMUX_SOCK", &sock)
        .spawn()
        .unwrap();
    wait_until("socket", Duration::from_secs(5), || {
        std::os::unix::net::UnixStream::connect(&sock).is_ok()
    });
    let mut s = std::os::unix::net::UnixStream::connect(&sock).unwrap();
    s.write_all(b"list\n").unwrap();
    let mut r = String::new();
    s.read_to_string(&mut r).unwrap();
    assert_eq!(r, "OK\n");
    let _ = c.kill();
    let _ = c.wait();
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn exited_sessions_are_pruned_at_the_cap() {
    let d = Daemon::start();
    for i in 0..300 {
        d.ok(&format!("spawn e{i} true"));
    }
    wait_until("all exited", Duration::from_secs(20), || {
        !d.ok("list").contains("running")
    });
    d.ok("spawn last sleep 60"); // pruning happens on spawn
    let list = d.ok("list");
    assert!(
        list.lines().count() <= 257,
        "{} sessions kept",
        list.lines().count()
    );
    assert!(list.contains("last\t") && list.contains("e299\t") && !list.contains("e0\t"));
}

#[test]
fn small_edge_cases() {
    let d = Daemon::start();
    d.bash("s");
    assert_eq!(d.ok("history s 0"), "\n");
    assert!(d.err("rename s s").contains("already exists"));
    d.ok("send s"); // empty input is fine
    d.ok("keys s");
    d.ok("send s echo edge-$((1+1))\\r");
    d.wait("s", "edge-2");
    assert!(
        d.err("wait s 0 never-there")
            .contains("timed out after 0 ms")
    );
    assert_eq!(d.ok("wait s 0 edge-2"), "edge-2\n");
    assert!(d.err("run s abc x").contains("usage"));
    assert!(d.err("run nope 100 x").contains("no session"));
    assert!(d.err("settle").contains("no session"));
    assert!(d.err("history").contains("no session"));
    assert!(d.err("resize s 10").contains("usage"));
}

#[test]
fn run_output_larger_than_history_does_not_break() {
    let d = Daemon::start();
    d.bash("s");
    let out = d.ok("run s 60000 seq 1 15000");
    let nums: Vec<u32> = out.lines().filter_map(|l| l.parse().ok()).collect();
    assert_eq!(nums.last(), Some(&15000));
    assert!(nums.len() >= 9_000, "{} lines", nums.len());
    assert!(out.ends_with("[xumux] quiet\n"));
}
