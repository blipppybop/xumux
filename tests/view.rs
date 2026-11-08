//! View mode inside `xumux attach`: paging history without the host scrollback.

mod common;
use common::*;

use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const MOUSE_OFF: &str = "\x1b[?1000;1002;1003;1006l";
const PASTE_OFF: &str = "\x1b[?2004l";

struct Client {
    child: Child,
    buf: Arc<Mutex<String>>,
    reader: Option<thread::JoinHandle<()>>,
}

impl Client {
    fn spawn(d: &Daemon, name: &str) -> Self {
        let mut child = Command::new(BIN)
            .args(["attach", name])
            .env("XUMUX_SOCK", &d.sock)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let buf = Arc::new(Mutex::new(String::new()));
        let buf2 = buf.clone();
        let reader = thread::spawn(move || {
            let mut tmp = [0u8; 8192];
            while let Ok(n @ 1..) = stdout.read(&mut tmp) {
                buf2.lock()
                    .unwrap()
                    .push_str(&String::from_utf8_lossy(&tmp[..n]));
            }
        });
        Client {
            child,
            buf,
            reader: Some(reader),
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        let sin = self.child.stdin.as_mut().unwrap();
        sin.write_all(bytes).unwrap();
        sin.flush().unwrap();
    }

    fn text(&self) -> String {
        self.buf.lock().unwrap().clone()
    }

    fn wait(&self, what: &str, mut f: impl FnMut(&str) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let got = self.text();
            if f(&got) {
                return;
            }
            if Instant::now() >= deadline {
                let n = got.len().saturating_sub(1800);
                panic!("timed out waiting for {what}. output tail:\n{}", &got[n..]);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn finish(mut self) -> (String, String) {
        let out = self.child.wait_with_output().unwrap();
        if let Some(reader) = self.reader.take() {
            reader.join().unwrap();
        }
        let stdout = self.buf.lock().unwrap().clone();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        (stdout, stderr)
    }
}

/// `view: lines a-b of n` spans, in the order they were drawn.
fn spans(s: &str) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(i) = rest.find("view: lines ") {
        rest = &rest[i + "view: lines ".len()..];
        let Some((a, next)) = rest.split_once('-') else {
            break;
        };
        let Some((b, next)) = next.split_once(" of ") else {
            break;
        };
        let n: String = next.chars().take_while(|c| c.is_ascii_digit()).collect();
        let (Ok(a), Ok(b), Ok(n)) = (a.parse::<usize>(), b.parse::<usize>(), n.parse::<usize>())
        else {
            break;
        };
        out.push((a, b, n));
    }
    out
}

#[test]
fn view_pages_old_lines_searches_and_returns_to_the_live_screen() {
    let d = Daemon::start();
    d.bash("s");
    // Zero-padded so ROW-003 is not a substring of ROW-030 or ROW-300.
    d.ok("send s for i in $(seq -w 1 300); do echo ROW-$i; done\\r");
    d.wait("s", "ROW-300");
    d.ok("settle s 10000");
    let hist = d.ok("history s");
    assert!(hist.contains("ROW-003\n"), "scrollback missing");
    assert!(hist.contains("ROW-300\n"));

    let mut c = Client::spawn(&d, "s");
    c.wait("initial paint", |s| s.contains("ROW-300"));
    c.write(&[0x1e]); // Ctrl-^
    c.wait("view status", |s| {
        spans(s).first().is_some_and(|z| z.2 > 250 && z.1 == z.2)
    });
    let first = spans(&c.text());
    let (a0, b0, n0) = *first.first().expect("status");
    assert!(
        a0 > 1 && b0 == n0 && n0 > 250,
        "not bottom-anchored: {first:?}"
    );
    assert!(
        !c.text().contains("ROW-003\x1b[K"),
        "ROW-003 is off the bottom page and must not be drawn yet"
    );
    assert_eq!(d.info("s", "attached"), "1");

    // Typed while viewing: must not reach the shell. `echo NOPE-9` has none of
    // the view-mode keys (q g G j k /).
    c.write(b"echo NOPE-9\r");
    d.ok("settle s 2000");
    assert!(
        !d.screen("s").contains("NOPE"),
        "view keys reached the program: {}",
        d.screen("s")
    );

    c.write(b"k");
    c.wait("k moves up", |s| {
        spans(s).last().is_some_and(|z| z.0 == a0 - 1)
    });
    c.write(b"\x1b[A");
    c.wait("arrow up", |s| {
        spans(s).last().is_some_and(|z| z.0 == a0 - 2)
    });
    c.write(b"\x1b[5~"); // PgUp
    c.wait("page up", |s| {
        spans(s).last().is_some_and(|z| z.0 + 1 < a0 - 2)
    });
    let a_page = spans(&c.text()).last().unwrap().0;
    assert!(
        !c.text().contains("ROW-003\x1b[K"),
        "one page up still should not show ROW-003"
    );
    c.write(b"j");
    c.wait("j moves down", |s| {
        spans(s).last().is_some_and(|z| z.0 == a_page + 1)
    });

    c.write(b"/ROW-003\r");
    c.wait("search hit", |s| s.contains("\x1b[1;1HROW-003\x1b[K"));

    c.write(b"g");
    c.wait("home", |s| spans(s).last().is_some_and(|z| z.0 == 1));
    c.write(b"G");
    c.wait("end", |s| {
        spans(s).last().is_some_and(|z| z.1 == z.2 && z.0 > 1)
    });

    d.ok("send s echo DURING-$((4+4))\\r");
    d.wait("s", "DURING-8");
    c.write(b"q");
    c.wait("restored live output", |s| {
        let Some(off) = s.find(MOUSE_OFF) else {
            return false;
        };
        let after = &s[off..];
        match (after.find("\x1b[2J"), after.find("DURING-8")) {
            (Some(j), Some(d)) => d > j,
            _ => false,
        }
    });
    assert!(c.text().contains(PASTE_OFF));
    assert!(!c.text().contains("\x1b[3J"));

    c.write(b"echo BACK-$((2+5))\r");
    d.wait("s", "BACK-7");
    c.write(&[0x1d]);
    let (stdout, stderr) = c.finish();
    assert!(stdout.contains("[xumux: detached]"), "{stdout:?}");
    assert!(stderr.contains("Ctrl-^") && stderr.contains("Ctrl-]"));
    let screen = d.screen("s");
    assert!(screen.contains("BACK-7"), "{screen}");
    assert!(!screen.contains("NOPE"), "{screen}");
    assert!(!d.ok("history s").contains("NOPE"));
    wait_until("detached", Duration::from_secs(5), || {
        d.info("s", "attached") == "0"
    });
}

#[test]
fn ctrl_bracket_detaches_from_inside_view_mode() {
    let d = Daemon::start();
    d.bash("s");
    d.ok("send s echo READY-$((1+2))\\r");
    d.wait("s", "READY-3");
    let mut c = Client::spawn(&d, "s");
    c.wait("paint", |s| s.contains("READY-3"));
    c.write(&[0x1e]);
    c.wait("view", |s| s.contains("view: lines "));
    assert_eq!(d.info("s", "attached"), "1");
    c.write(b"echo GONE-1\r");
    c.write(&[0x1d]);
    let (stdout, stderr) = c.finish();
    assert!(stdout.contains("view: lines "), "{stdout:?}");
    assert!(stdout.contains("[xumux: detached]"));
    assert!(stderr.contains("Ctrl-^"));
    assert!(!stdout.contains("\x1b[3J"));
    wait_until("detached", Duration::from_secs(5), || {
        d.info("s", "attached") == "0"
    });
    let screen = d.screen("s");
    assert!(screen.contains("READY-3"), "{screen}");
    assert!(!screen.contains("GONE"), "{screen}");
    assert!(d.ok("list").contains("running"));
    d.alive();
}

#[test]
fn view_mode_turns_mouse_and_paste_off_then_restores_them() {
    let d = Daemon::start();
    d.bash("s");
    d.ok(
        r"send s printf '\\033[?1000h\\033[?1002h\\033[?1003h\\033[?1006h\\033[?2004h'; echo MODES-$((1+1))\r",
    );
    d.wait("s", "MODES-2");
    d.ok("settle s 5000");

    let mut c = Client::spawn(&d, "s");
    c.wait("modes painted", |s| {
        s.contains("\x1b[?1000h") && s.contains("\x1b[?2004h")
    });
    c.write(&[0x1e]);
    c.wait("mouse off", |s| {
        s.contains(MOUSE_OFF) && s.contains(PASTE_OFF)
    });
    let entered = c.text();
    let off = entered.find(MOUSE_OFF).unwrap();
    assert!(
        entered[..off].contains("\x1b[?1000h") && entered[..off].contains("\x1b[?2004h"),
        "program modes were not applied on attach"
    );
    // Ctrl-^ again leaves view mode (same as q).
    c.write(&[0x1e]);
    c.wait("modes restored", |s| {
        let after = &s[s.find(MOUSE_OFF).unwrap() + MOUSE_OFF.len()..];
        ["1000", "1002", "1003", "1006", "2004"]
            .iter()
            .all(|m| after.contains(&format!("\x1b[?{m}h")))
            && after.contains("\x1b[2J")
    });
    c.write(b"echo STILL-$((3+3))\r");
    d.wait("s", "STILL-6");
    c.write(&[0x1d]);
    let (stdout, _) = c.finish();
    assert!(!stdout.contains("\x1b[3J"));
    assert!(d.screen("s").contains("STILL-6"));
}

#[test]
fn alt_screen_view_repaints_and_restores_modes() {
    let d = Daemon::start();
    d.bash("s");
    d.ok(r"send s printf '\\033[?1049h\\033[?1000h\\033[?1006h\\033[?2004h'; echo ALT-$((2+2))\r");
    d.wait("s", "ALT-4");
    assert_eq!(d.info("s", "alt_screen"), "yes");

    let mut c = Client::spawn(&d, "s");
    c.wait("alt paint", |s| s.contains("\x1b[?1049h"));
    c.write(&[0x1e]);
    c.wait("view", |s| s.contains(MOUSE_OFF));
    d.ok("send s echo INALT-$((5+5))\\r");
    d.wait("s", "INALT-10");
    c.write(b"q");
    c.wait("alt restore", |s| {
        let Some(off) = s.find(MOUSE_OFF) else {
            return false;
        };
        let after = &s[off..];
        let Some(j) = after.find("\x1b[2J") else {
            return false;
        };
        let Some(alt) = after.find("\x1b[?1049h") else {
            return false;
        };
        let Some(mouse) = after.find("\x1b[?1000h") else {
            return false;
        };
        // Alt screen is entered before the clear, other modes after it,
        // and the repaint (not a raw replay) carries output from during view.
        alt < j && j < mouse && after[j..].contains("INALT-10") && after.contains("\x1b[?2004h")
    });
    c.write(&[0x1d]);
    let (stdout, _) = c.finish();
    assert!(!stdout.contains("\x1b[3J"));
    assert_eq!(d.info("s", "alt_screen"), "yes");
}
