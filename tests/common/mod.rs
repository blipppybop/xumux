//! Shared harness for the integration tests: each test gets its own daemon on
//! its own private socket, torn down (with every session) on drop.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{fs, thread};

pub const BIN: &str = env!("CARGO_BIN_EXE_xumux");
/// A predictable interactive shell: no rc files, prompt "$ ".
pub const BASH: &str = "env PS1='$ ' bash --norc --noprofile -i";

pub fn private_dir() -> PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let d = std::env::temp_dir().join(format!(
        "xumux-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&d);
    fs::DirBuilder::new().mode(0o700).create(&d).unwrap();
    d
}

pub fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

pub struct Daemon {
    pub child: Child,
    pub dir: PathBuf,
    pub sock: PathBuf,
}

impl Daemon {
    pub fn start() -> Daemon {
        let dir = private_dir();
        let sock = dir.join("s.sock");
        Daemon::start_at(dir, sock)
    }

    pub fn start_at(dir: PathBuf, sock: PathBuf) -> Daemon {
        let child = Command::new(BIN)
            .arg("serve")
            .env("XUMUX_SOCK", &sock)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        wait_until("daemon socket", Duration::from_secs(5), || {
            UnixStream::connect(&sock).is_ok()
        });
        Daemon { child, dir, sock }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn raw(&self, req: &[u8]) -> String {
        let mut s = UnixStream::connect(&self.sock).unwrap();
        s.write_all(req).unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        out
    }

    pub fn req(&self, line: &str) -> String {
        self.raw(format!("{line}\n").as_bytes())
    }

    /// Body of an OK reply; panics on ERR.
    pub fn ok(&self, line: &str) -> String {
        let r = self.req(line);
        match r.strip_prefix("OK\n") {
            Some(body) => body.to_string(),
            None => panic!("{line:?} -> {r:?}"),
        }
    }

    /// Message of an ERR reply; panics on OK.
    pub fn err(&self, line: &str) -> String {
        let r = self.req(line);
        match r.strip_prefix("ERR ") {
            Some(msg) => msg.trim_end().to_string(),
            None => panic!("expected ERR for {line:?}, got {r:?}"),
        }
    }

    pub fn wait(&self, name: &str, text: &str) {
        self.ok(&format!("wait {name} 10000 {text}"));
    }

    pub fn screen(&self, name: &str) -> String {
        self.ok(&format!("screen {name}"))
    }

    pub fn info(&self, name: &str, key: &str) -> String {
        let body = self.ok(&format!("info {name}"));
        body.lines()
            .find_map(|l| l.strip_prefix(&format!("{key}\t")))
            .unwrap_or_else(|| panic!("no {key} in {body}"))
            .to_string()
    }

    /// Spawn an interactive bash and wait for its prompt.
    pub fn bash(&self, name: &str) -> u32 {
        let pid = self.ok(&format!("spawn {name} {BASH}")).trim().parse().unwrap();
        self.wait(name, "$");
        pid
    }

    pub fn alive(&self) {
        assert!(self.req("list").starts_with("OK\n"), "daemon not answering");
    }

    /// Run the real CLI against this daemon.
    pub fn cli(&self, args: &[&str]) -> Output {
        Command::new(BIN)
            .args(args)
            .env("XUMUX_SOCK", &self.sock)
            .output()
            .unwrap()
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Ok(mut s) = UnixStream::connect(&self.sock) {
            let _ = s.write_all(b"shutdown\n");
            let mut sink = String::new();
            let _ = s.read_to_string(&mut sink);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// Running (not zombie) process?
pub fn proc_alive(pid: u32) -> bool {
    match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(s) => s
            .rfind(')')
            .and_then(|i| s[i + 1..].split_whitespace().next())
            .is_some_and(|st| st != "Z" && st != "X"),
        Err(_) => false,
    }
}

/// Live processes whose command line contains `needle`.
pub fn pids_matching(needle: &str) -> Vec<u32> {
    let me = std::process::id();
    fs::read_dir("/proc")
        .unwrap()
        .flatten()
        .filter_map(|e| e.file_name().to_str()?.parse::<u32>().ok())
        .filter(|&p| p != me && proc_alive(p))
        .filter(|p| {
            fs::read(format!("/proc/{p}/cmdline"))
                .map(|c| String::from_utf8_lossy(&c).replace('\0', " ").contains(needle))
                .unwrap_or(false)
        })
        .collect()
}

pub fn fd_count(pid: u32) -> usize {
    fs::read_dir(format!("/proc/{pid}/fd")).unwrap().count()
}

pub fn thread_count(pid: u32) -> usize {
    fs::read_dir(format!("/proc/{pid}/task")).unwrap().count()
}

pub fn mode(p: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(p).unwrap().permissions().mode() & 0o777
}

/// Deterministic xorshift PRNG (no dependencies, reproducible failures).
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}
