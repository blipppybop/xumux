//! xumux — a lean local terminal multiplexer / PTY broker. Zero dependencies.
//!
//! `xumux serve` runs the daemon on a Unix socket (mode 0600).
//! Every other subcommand is a client that sends one line and prints the reply.
//!
//! Wire protocol: one request line in, `OK\n<body>` or `ERR <msg>\n` out, then close.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{env, thread};

#[cfg(not(all(
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "x86",
        target_arch = "aarch64",
        target_arch = "arm",
        target_arch = "riscv64"
    )
)))]
compile_error!("xumux supports Linux on x86, x86_64, arm, aarch64 and riscv64 (ioctl numbers are arch-specific)");

const USAGE: &str = "\
usage:
  xumux serve                      run the daemon (foreground)
  xumux spawn NAME COMMAND...      start COMMAND (via sh -c) in a new PTY
  xumux send NAME TEXT             write TEXT; escapes: \\r \\n \\t \\e \\\\ \\xHH
  xumux screen NAME                print the current screen
  xumux list                       list sessions: name, pid, status, command
  xumux stats NAME                 CPU % and RSS of the session's process tree
  xumux resize NAME ROWS COLS      resize the PTY (1-1000 each)
  xumux kill NAME                  kill the session and forget it

socket: $XUMUX_SOCK, else $XDG_RUNTIME_DIR/xumux.sock
tip: most TUIs submit on carriage return:  xumux send grok 'hello\\r'
";

const ROWS: u16 = 40;
const COLS: u16 = 120;
const MAX_DIM: u16 = 1000;
const MAX_REQUEST: u64 = 1 << 20;
const USER_HZ: f64 = 100.0; // /proc reports CPU time in USER_HZ, fixed at 100 on Linux

struct Session {
    pid: u32,
    cmd: String,
    child: Mutex<Child>,
    writer: Arc<Mutex<File>>,
    master: File,
    term: Arc<Mutex<Term>>,
}

type Sessions = Arc<Mutex<HashMap<String, Arc<Session>>>>;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None | Some("-h" | "--help" | "help") => {
            eprint!("{USAGE}");
            std::process::exit(2)
        }
        Some("serve") => serve(),
        _ => client(&args),
    }
}

fn sock_path() -> PathBuf {
    if let Ok(p) = env::var("XUMUX_SOCK") {
        return p.into();
    }
    let dir = env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(dir).join("xumux.sock")
}

fn die(msg: impl std::fmt::Display) -> ! {
    eprintln!("xumux: {msg}");
    std::process::exit(1)
}

// ---------------------------------------------------------------- client

fn client(args: &[String]) {
    let path = sock_path();
    let mut stream = UnixStream::connect(&path)
        .unwrap_or_else(|e| die(format!("{}: {e} (is `xumux serve` running?)", path.display())));
    // Literal newlines would end the request line early; send them as escapes.
    let line = args.join(" ").replace('\r', "\\r").replace('\n', "\\n");
    stream
        .write_all(format!("{line}\n").as_bytes())
        .unwrap_or_else(|e| die(e));
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap_or_else(|e| die(e));
    match reply.strip_prefix("OK\n") {
        Some(body) => print!("{body}"),
        None => die(reply.strip_prefix("ERR ").unwrap_or(&reply).trim_end()),
    }
}

// ---------------------------------------------------------------- server

fn serve() {
    let path = sock_path();
    let listener = match UnixListener::bind(&path) {
        Ok(l) => l,
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            if UnixStream::connect(&path).is_ok() {
                die(format!("already running at {}", path.display()));
            }
            fs::remove_file(&path).unwrap_or_else(|e| die(e));
            UnixListener::bind(&path).unwrap_or_else(|e| die(e))
        }
        Err(e) => die(format!("{}: {e}", path.display())),
    };
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap_or_else(|e| die(e));
    eprintln!("xumux listening on {}", path.display());

    let sessions: Sessions = Default::default();
    for conn in listener.incoming() {
        let Ok(stream) = conn else { continue };
        let sessions = sessions.clone();
        thread::spawn(move || handle(stream, &sessions));
    }
}

fn handle(stream: UnixStream, sessions: &Sessions) {
    let mut line = String::new();
    if BufReader::new((&stream).take(MAX_REQUEST)).read_line(&mut line).is_err() {
        return;
    }
    let reply = match dispatch(line.trim_end_matches(['\r', '\n']), sessions) {
        Ok(body) => format!("OK\n{body}"),
        Err(e) => format!("ERR {e}\n"),
    };
    let _ = (&stream).write_all(reply.as_bytes());
}

fn err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

/// Split off the first space-separated word; the rest is returned untouched.
fn split1(s: &str) -> (&str, &str) {
    let s = s.trim_start_matches(' ');
    s.split_once(' ').unwrap_or((s, ""))
}

fn get(sessions: &Sessions, name: &str) -> Result<Arc<Session>, String> {
    sessions
        .lock()
        .unwrap()
        .get(name)
        .cloned()
        .ok_or_else(|| format!("no session '{name}'"))
}

fn dim(s: &str) -> Result<u16, String> {
    match s.parse::<u16>() {
        Ok(n @ 1..=MAX_DIM) => Ok(n),
        _ => Err(format!("size must be 1-{MAX_DIM}, got '{s}'")),
    }
}

fn dispatch(line: &str, sessions: &Sessions) -> Result<String, String> {
    let (cmd, rest) = split1(line);
    match cmd {
        "spawn" => {
            let (name, command) = split1(rest);
            if name.is_empty() || command.trim().is_empty() {
                return Err("usage: spawn NAME COMMAND...".into());
            }
            spawn(name, command, sessions)
        }
        "send" => {
            let (name, text) = split1(rest);
            let s = get(sessions, name)?;
            let bytes = unescape(text)?;
            let mut w = s.writer.lock().unwrap();
            w.write_all(&bytes).and_then(|_| w.flush()).map_err(err)?;
            Ok(String::new())
        }
        "screen" => {
            let s = get(sessions, rest.trim())?;
            let contents = s.term.lock().unwrap().contents();
            Ok(format!("{contents}\n"))
        }
        "list" => {
            let map = sessions.lock().unwrap();
            let mut names: Vec<_> = map.keys().collect();
            names.sort();
            let mut out = String::new();
            for n in names {
                let s = &map[n];
                let status = match s.child.lock().unwrap().try_wait() {
                    Ok(None) => "running".into(),
                    Ok(Some(st)) => match (st.code(), st.signal()) {
                        (Some(c), _) => format!("exited({c})"),
                        (_, Some(sig)) => format!("killed(sig {sig})"),
                        _ => "exited".into(),
                    },
                    Err(_) => "unknown".into(),
                };
                out += &format!("{n}\t{}\t{status}\t{}\n", s.pid, s.cmd);
            }
            Ok(out)
        }
        "stats" => stats(get(sessions, rest.trim())?.pid),
        "resize" => {
            let mut it = rest.split_whitespace();
            let (Some(name), Some(r), Some(c)) = (it.next(), it.next(), it.next()) else {
                return Err("usage: resize NAME ROWS COLS".into());
            };
            let (rows, cols) = (dim(r)?, dim(c)?);
            let s = get(sessions, name)?;
            sys::set_size(&s.master, rows, cols).map_err(err)?;
            s.term.lock().unwrap().resize(rows.into(), cols.into());
            Ok(String::new())
        }
        "kill" => {
            let name = rest.trim();
            let s = sessions
                .lock()
                .unwrap()
                .remove(name)
                .ok_or_else(|| format!("no session '{name}'"))?;
            let mut child = s.child.lock().unwrap();
            if let Ok(None) = child.try_wait() {
                child.kill().map_err(err)?;
                let _ = child.wait();
            }
            Ok(String::new())
        }
        "" => Err("empty request".into()),
        other => Err(format!("unknown command '{other}'")),
    }
}

fn open_pty_dev(path: &str) -> io::Result<File> {
    // std always adds O_CLOEXEC, so PTY fds never leak into other sessions' children.
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(sys::O_NOCTTY)
        .open(path)
}

fn spawn(name: &str, command: &str, sessions: &Sessions) -> Result<String, String> {
    if sessions.lock().unwrap().contains_key(name) {
        return Err(format!("session '{name}' already exists"));
    }
    let master = open_pty_dev("/dev/ptmx").map_err(err)?;
    let n = sys::unlock_and_number(&master).map_err(err)?;
    sys::set_size(&master, ROWS, COLS).map_err(err)?;
    let slave = open_pty_dev(&format!("/dev/pts/{n}")).map_err(err)?;

    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(format!("exec {command}"))
        .env("TERM", "xterm-256color")
        .stdin(Stdio::from(slave.try_clone().map_err(err)?))
        .stdout(Stdio::from(slave.try_clone().map_err(err)?))
        .stderr(Stdio::from(slave));
    if let Ok(home) = env::var("HOME") {
        cmd.current_dir(home);
    }
    // SAFETY: the hook only makes two async-signal-safe syscalls (setsid, ioctl)
    // and does not allocate.
    unsafe { cmd.pre_exec(sys::make_controlling_tty) };
    let mut child = cmd.spawn().map_err(err)?;
    drop(cmd); // close our copies of the slave so reads hit EOF/EIO when the child exits

    let mut reader = master.try_clone().map_err(err)?;
    let writer = Arc::new(Mutex::new(master.try_clone().map_err(err)?));
    let term = Arc::new(Mutex::new(Term::new(ROWS.into(), COLS.into())));
    let (feed, answer) = (term.clone(), writer.clone());
    thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            let n = match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break, // EIO once the child side is closed
            };
            let reply = {
                let mut t = feed.lock().unwrap();
                t.feed(&buf[..n]);
                std::mem::take(&mut t.reply)
            };
            if !reply.is_empty() {
                let _ = answer.lock().unwrap().write_all(&reply);
            }
        }
    });

    let pid = child.id();
    let mut map = sessions.lock().unwrap();
    if map.contains_key(name) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(format!("session '{name}' already exists"));
    }
    map.insert(
        name.to_string(),
        Arc::new(Session {
            pid,
            cmd: command.to_string(),
            child: Mutex::new(child),
            writer,
            master,
            term,
        }),
    );
    Ok(format!("{pid}\n"))
}

fn unescape(s: &str) -> Result<Vec<u8>, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            out.push(b[i]);
            i += 1;
            continue;
        }
        i += 1;
        match b.get(i) {
            Some(b'n') => out.push(b'\n'),
            Some(b'r') => out.push(b'\r'),
            Some(b't') => out.push(b'\t'),
            Some(b'e') => out.push(0x1b),
            Some(b'\\') => out.push(b'\\'),
            Some(b'x') => {
                let hex = s.get(i + 1..i + 3).ok_or("bad \\x escape")?;
                out.push(u8::from_str_radix(hex, 16).map_err(|_| "bad \\x escape")?);
                i += 2;
            }
            _ => return Err(format!("bad escape at byte {}", i - 1)),
        }
        i += 1;
    }
    Ok(out)
}

// ---------------------------------------------------------------- kernel interface
//
// All raw syscalls live here (the pre_exec registration in spawn() is the only
// other `unsafe`). Two libc symbols (already linked by std) and four
// ioctl numbers from <asm-generic/ioctls.h>.

mod sys {
    use std::ffi::{c_int, c_ulong};
    use std::io;
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
        fn setsid() -> c_int;
    }

    pub const O_NOCTTY: i32 = 0o400;
    const TIOCSCTTY: c_ulong = 0x540E;
    const TIOCSWINSZ: c_ulong = 0x5414;
    const TIOCGPTN: c_ulong = 0x8004_5430; // _IOR('T', 0x30, unsigned int)
    const TIOCSPTLCK: c_ulong = 0x4004_5431; // _IOW('T', 0x31, int)

    #[repr(C)]
    struct Winsize {
        rows: u16,
        cols: u16,
        xpixel: u16,
        ypixel: u16,
    }

    fn check(r: c_int) -> io::Result<()> {
        if r == -1 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    /// Unlock a freshly opened /dev/ptmx and return its /dev/pts number.
    pub fn unlock_and_number(master: &impl AsRawFd) -> io::Result<u32> {
        let unlock: c_int = 0;
        let mut n: u32 = 0;
        // SAFETY: valid open fd; the pointers refer to live locals of the types
        // these requests expect.
        unsafe {
            check(ioctl(master.as_raw_fd(), TIOCSPTLCK, &unlock as *const c_int))?;
            check(ioctl(master.as_raw_fd(), TIOCGPTN, &mut n as *mut u32))?;
        }
        Ok(n)
    }

    pub fn set_size(master: &impl AsRawFd, rows: u16, cols: u16) -> io::Result<()> {
        let ws = Winsize { rows, cols, xpixel: 0, ypixel: 0 };
        // SAFETY: valid open fd; pointer to a live, correctly laid out winsize.
        unsafe { check(ioctl(master.as_raw_fd(), TIOCSWINSZ, &ws as *const Winsize)) }
    }

    /// Runs in the forked child (pre_exec), after stdio is the PTY slave:
    /// start a new session and adopt stdin as the controlling terminal.
    /// Async-signal-safe: no allocation, only raw syscalls.
    pub fn make_controlling_tty() -> io::Result<()> {
        // SAFETY: plain syscalls with integer arguments.
        unsafe {
            check(setsid())?;
            check(ioctl(0, TIOCSCTTY, 0 as c_int))
        }
    }
}

// ---------------------------------------------------------------- terminal emulator
//
// Enough of VT100/xterm for TUIs: cursor movement, erase, insert/delete,
// scroll regions, alternate screen, UTF-8 with wide characters, and replies to
// cursor-position / device-attribute queries. Colors and styles are parsed and
// dropped: `screen` returns text.

const WIDE_TAIL: char = '\0'; // right half of a double-width character

#[derive(Clone, Copy)]
enum State {
    Ground,
    Esc,
    Csi,
    Osc,
    OscEsc,
    Str,
    StrEsc,
    Charset,
}

struct Term {
    rows: usize,
    cols: usize,
    grid: Vec<char>,
    main: Option<Vec<char>>, // main screen, saved while the alternate screen is up
    row: usize,
    col: usize,
    wrap_pending: bool,
    top: usize, // scroll region, inclusive
    bot: usize,
    saved: (usize, usize),
    state: State,
    params: String,
    utf8: Vec<u8>,
    utf8_need: usize,
    reply: Vec<u8>, // bytes to send back to the program
}

fn width(c: char) -> usize {
    match c as u32 {
        0..=0x2FF => 1,
        0x300..=0x36F | 0x200B..=0x200F | 0x20D0..=0x20FF | 0xFE00..=0xFE0F => 0,
        0x1100..=0x115F
        | 0x2E80..=0x303E
        | 0x3041..=0x4DBF
        | 0x4E00..=0xA4CF
        | 0xAC00..=0xD7A3
        | 0xF900..=0xFAFF
        | 0xFE30..=0xFE4F
        | 0xFF00..=0xFF60
        | 0xFFE0..=0xFFE6
        | 0x1F300..=0x1F64F
        | 0x1F680..=0x1F6FF
        | 0x1F900..=0x1F9FF
        | 0x20000..=0x3FFFD => 2,
        _ => 1,
    }
}

impl Term {
    fn new(rows: usize, cols: usize) -> Self {
        Term {
            rows,
            cols,
            grid: vec![' '; rows * cols],
            main: None,
            row: 0,
            col: 0,
            wrap_pending: false,
            top: 0,
            bot: rows - 1,
            saved: (0, 0),
            state: State::Ground,
            params: String::new(),
            utf8: Vec::new(),
            utf8_need: 0,
            reply: Vec::new(),
        }
    }

    fn contents(&self) -> String {
        let mut out = String::new();
        for line in self.grid.chunks(self.cols) {
            let s: String = line.iter().filter(|&&c| c != WIDE_TAIL).collect();
            out.push_str(s.trim_end_matches(' '));
            out.push('\n');
        }
        out.truncate(out.trim_end_matches('\n').len());
        out
    }

    fn resize(&mut self, rows: usize, cols: usize) {
        let (orows, ocols) = (self.rows, self.cols);
        let copy = |old: &[char]| {
            let mut g = vec![' '; rows * cols];
            for r in 0..rows.min(orows) {
                for c in 0..cols.min(ocols) {
                    g[r * cols + c] = old[r * ocols + c];
                }
            }
            g
        };
        self.main = self.main.as_deref().map(copy);
        self.grid = copy(&self.grid);
        self.rows = rows;
        self.cols = cols;
        self.row = self.row.min(rows - 1);
        self.col = self.col.min(cols - 1);
        self.top = 0;
        self.bot = rows - 1;
        self.wrap_pending = false;
    }

    fn feed(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.byte(b);
        }
    }

    fn byte(&mut self, b: u8) {
        use State::*;
        match self.state {
            Ground => self.ground(b),
            Esc => {
                self.state = Ground;
                self.esc(b);
            }
            Csi => match b {
                0x20..=0x3f => {
                    if self.params.len() < 64 {
                        self.params.push(b as char);
                    }
                }
                0x40..=0x7e => {
                    self.state = Ground;
                    self.csi(b);
                }
                0x1b => self.state = Esc,
                0x18 | 0x1a => self.state = Ground,
                _ => {}
            },
            Osc => match b {
                0x07 => self.state = Ground,
                0x1b => self.state = OscEsc,
                _ => {}
            },
            Str => {
                if b == 0x1b {
                    self.state = StrEsc;
                }
            }
            // ESC inside a string: `ESC \` terminates it; anything else starts a new escape.
            OscEsc | StrEsc => {
                self.state = Esc;
                self.byte(b);
            }
            Charset => self.state = Ground,
        }
    }

    fn ground(&mut self, b: u8) {
        if self.utf8_need > 0 {
            if b & 0xC0 == 0x80 {
                self.utf8.push(b);
                if self.utf8.len() == self.utf8_need {
                    let c = std::str::from_utf8(&self.utf8)
                        .ok()
                        .and_then(|s| s.chars().next())
                        .unwrap_or('\u{FFFD}');
                    self.utf8.clear();
                    self.utf8_need = 0;
                    self.print(c);
                }
                return;
            }
            self.utf8.clear();
            self.utf8_need = 0;
            self.print('\u{FFFD}');
        }
        match b {
            0x1b => self.state = State::Esc,
            b'\r' => {
                self.col = 0;
                self.wrap_pending = false;
            }
            b'\n' | 0x0b | 0x0c => self.index(),
            0x08 => {
                self.col = self.col.saturating_sub(1);
                self.wrap_pending = false;
            }
            b'\t' => self.col = ((self.col / 8 + 1) * 8).min(self.cols - 1),
            0x00..=0x1f | 0x7f => {}
            0x20..=0x7e => self.print(b as char),
            0xC2..=0xF4 => {
                self.utf8.push(b);
                self.utf8_need = match b {
                    0xC2..=0xDF => 2,
                    0xE0..=0xEF => 3,
                    _ => 4,
                };
            }
            _ => self.print('\u{FFFD}'),
        }
    }

    fn esc(&mut self, b: u8) {
        match b {
            b'[' => {
                self.params.clear();
                self.state = State::Csi;
            }
            b']' => self.state = State::Osc,
            b'P' | b'X' | b'^' | b'_' => self.state = State::Str,
            b'(' | b')' | b'*' | b'+' | b'-' | b'.' | b'/' | b'#' | b'%' | b' ' => {
                self.state = State::Charset
            }
            b'7' => self.saved = (self.row, self.col),
            b'8' => self.restore(),
            b'D' => self.index(),
            b'E' => {
                self.col = 0;
                self.index();
            }
            b'M' => self.reverse_index(),
            b'c' => *self = Term::new(self.rows, self.cols),
            _ => {}
        }
    }

    fn csi(&mut self, f: u8) {
        let params = std::mem::take(&mut self.params);
        if params.bytes().any(|c| (0x20..=0x2f).contains(&c)) {
            return; // sequences with intermediates (cursor style etc.) are ignored
        }
        let lead = params.bytes().next().filter(|c| b"?<=>".contains(c));
        let nums: Vec<usize> = params
            .trim_start_matches(['?', '<', '=', '>'])
            .split(';')
            .map(|p| p.split(':').next().unwrap_or("").parse().unwrap_or(0).min(65535))
            .collect();
        let p = |i: usize| nums.get(i).copied().unwrap_or(0);
        let n = p(0).max(1);
        let (rows, cols) = (self.rows, self.cols);
        let cur = self.row * cols + self.col;
        let line = self.row * cols;
        if f != b'm' {
            self.wrap_pending = false;
        }
        match (lead, f) {
            (Some(b'?'), b'h' | b'l') => {
                for &m in &nums {
                    self.mode(m, f == b'h');
                }
            }
            (Some(b'>'), b'c') => self.reply.extend_from_slice(b"\x1b[>0;0;0c"),
            (Some(_), _) => {}
            (None, b'A') => self.up(n),
            (None, b'B' | b'e') => self.down(n),
            (None, b'C' | b'a') => self.col = (self.col + n).min(cols - 1),
            (None, b'D') => self.col = self.col.saturating_sub(n),
            (None, b'E') => {
                self.down(n);
                self.col = 0;
            }
            (None, b'F') => {
                self.up(n);
                self.col = 0;
            }
            (None, b'G' | b'`') => self.col = (n - 1).min(cols - 1),
            (None, b'd') => self.row = (n - 1).min(rows - 1),
            (None, b'H' | b'f') => {
                self.row = (n - 1).min(rows - 1);
                self.col = (p(1).max(1) - 1).min(cols - 1);
            }
            (None, b'J') => match p(0) {
                0 => self.grid[cur..].fill(' '),
                1 => self.grid[..=cur].fill(' '),
                2 | 3 => self.grid.fill(' '),
                _ => {}
            },
            (None, b'K') => match p(0) {
                0 => self.grid[cur..line + cols].fill(' '),
                1 => self.grid[line..=cur].fill(' '),
                2 => self.grid[line..line + cols].fill(' '),
                _ => {}
            },
            (None, b'L' | b'M') => {
                if (self.top..=self.bot).contains(&self.row) {
                    if f == b'L' {
                        self.scroll_down(self.row, self.bot, n);
                    } else {
                        self.scroll_up(self.row, self.bot, n);
                    }
                    self.col = 0;
                }
            }
            (None, b'@') => {
                let s = &mut self.grid[cur..line + cols];
                let k = n.min(s.len());
                s.copy_within(..s.len() - k, k);
                s[..k].fill(' ');
            }
            (None, b'P') => {
                let s = &mut self.grid[cur..line + cols];
                let k = n.min(s.len());
                let len = s.len();
                s.copy_within(k.., 0);
                s[len - k..].fill(' ');
            }
            (None, b'X') => self.grid[cur..cur + n.min(cols - self.col)].fill(' '),
            (None, b'S') => self.scroll_up(self.top, self.bot, n),
            (None, b'T') => self.scroll_down(self.top, self.bot, n),
            (None, b'r') => {
                let t = n - 1;
                let b = if p(1) == 0 { rows } else { p(1).min(rows) } - 1;
                if t < b {
                    self.top = t;
                    self.bot = b;
                    self.row = 0;
                    self.col = 0;
                }
            }
            (None, b's') => self.saved = (self.row, self.col),
            (None, b'u') => self.restore(),
            (None, b'n') => match p(0) {
                5 => self.reply.extend_from_slice(b"\x1b[0n"),
                6 => {
                    let r = format!("\x1b[{};{}R", self.row + 1, self.col + 1);
                    self.reply.extend_from_slice(r.as_bytes());
                }
                _ => {}
            },
            (None, b'c') if p(0) == 0 => self.reply.extend_from_slice(b"\x1b[?1;2c"),
            _ => {} // SGR (colors) and everything else: ignored
        }
    }

    fn mode(&mut self, m: usize, on: bool) {
        if !matches!(m, 47 | 1047 | 1049) {
            return;
        }
        if on && self.main.is_none() {
            if m == 1049 {
                self.saved = (self.row, self.col);
            }
            let blank = vec![' '; self.rows * self.cols];
            self.main = Some(std::mem::replace(&mut self.grid, blank));
        } else if !on && let Some(main) = self.main.take() {
            self.grid = main;
            if m == 1049 {
                self.restore();
            }
        }
    }

    fn restore(&mut self) {
        self.row = self.saved.0.min(self.rows - 1);
        self.col = self.saved.1.min(self.cols - 1);
        self.wrap_pending = false;
    }

    fn up(&mut self, n: usize) {
        let min = if self.row >= self.top { self.top } else { 0 };
        self.row = self.row.saturating_sub(n).max(min);
    }

    fn down(&mut self, n: usize) {
        let max = if self.row <= self.bot { self.bot } else { self.rows - 1 };
        self.row = (self.row + n).min(max);
    }

    fn index(&mut self) {
        self.wrap_pending = false;
        if self.row == self.bot {
            self.scroll_up(self.top, self.bot, 1);
        } else if self.row + 1 < self.rows {
            self.row += 1;
        }
    }

    fn reverse_index(&mut self) {
        self.wrap_pending = false;
        if self.row == self.top {
            self.scroll_down(self.top, self.bot, 1);
        } else {
            self.row = self.row.saturating_sub(1);
        }
    }

    fn scroll_up(&mut self, t: usize, b: usize, n: usize) {
        let (n, c) = (n.min(b + 1 - t), self.cols);
        self.grid.copy_within((t + n) * c..(b + 1) * c, t * c);
        self.grid[(b + 1 - n) * c..(b + 1) * c].fill(' ');
    }

    fn scroll_down(&mut self, t: usize, b: usize, n: usize) {
        let (n, c) = (n.min(b + 1 - t), self.cols);
        self.grid.copy_within(t * c..(b + 1 - n) * c, (t + n) * c);
        self.grid[t * c..(t + n) * c].fill(' ');
    }

    fn print(&mut self, c: char) {
        let w = width(c);
        if w == 0 {
            return; // combining marks are dropped
        }
        if self.wrap_pending {
            self.col = 0;
            self.index();
        }
        if w == 2 && self.col + 1 >= self.cols {
            if self.cols < 2 {
                return;
            }
            self.put(self.col, ' ');
            self.col = 0;
            self.index();
        }
        self.put(self.col, c);
        if w == 2 {
            self.put(self.col + 1, WIDE_TAIL);
        }
        self.col += w;
        if self.col >= self.cols {
            self.col = self.cols - 1;
            self.wrap_pending = true;
        }
    }

    /// Write one cell without leaving half of an overwritten wide character behind.
    fn put(&mut self, col: usize, c: char) {
        let i = self.row * self.cols + col;
        if self.grid[i] == WIDE_TAIL && col > 0 && c != WIDE_TAIL {
            self.grid[i - 1] = ' ';
        }
        if col + 1 < self.cols && self.grid[i + 1] == WIDE_TAIL {
            self.grid[i + 1] = ' ';
        }
        self.grid[i] = c;
    }
}

// ---------------------------------------------------------------- /proc stats

/// (ppid, utime + stime in ticks)
fn proc_stat(pid: u32) -> Option<(u32, u64)> {
    let s = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // comm may contain spaces/parens, so parse after the last ')'
    let f: Vec<&str> = s[s.rfind(')')? + 1..].split_whitespace().collect();
    let ticks = f.get(11)?.parse::<u64>().ok()? + f.get(12)?.parse::<u64>().ok()?;
    Some((f.get(1)?.parse().ok()?, ticks))
}

fn rss_kib(pid: u32) -> u64 {
    fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|s| {
            let line = s.lines().find(|l| l.starts_with("VmRSS:"))?;
            line.split_whitespace().nth(1)?.parse().ok()
        })
        .unwrap_or(0)
}

/// root and all its descendants, with their CPU ticks
fn tree(root: u32) -> HashMap<u32, u64> {
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut ticks = HashMap::new();
    for entry in fs::read_dir("/proc").into_iter().flatten().flatten() {
        let Some(pid) = entry.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        if let Some((ppid, t)) = proc_stat(pid) {
            children.entry(ppid).or_default().push(pid);
            ticks.insert(pid, t);
        }
    }
    let mut out = HashMap::new();
    let mut seen = HashSet::new();
    let mut stack = vec![root];
    while let Some(p) = stack.pop() {
        if !seen.insert(p) {
            continue;
        }
        if let Some(&t) = ticks.get(&p) {
            out.insert(p, t);
        }
        stack.extend(children.get(&p).into_iter().flatten());
    }
    out
}

fn stats(pid: u32) -> Result<String, String> {
    const WINDOW: Duration = Duration::from_millis(500);
    let before = tree(pid);
    if before.is_empty() {
        return Err(format!("process {pid} is gone"));
    }
    thread::sleep(WINDOW);
    let after = tree(pid);
    let dticks: u64 = after
        .iter()
        .map(|(p, t)| t.saturating_sub(*before.get(p).unwrap_or(&0)))
        .sum();
    let cpu = dticks as f64 / USER_HZ / WINDOW.as_secs_f64() * 100.0;
    let rss: u64 = after.keys().map(|&p| rss_kib(p)).sum();
    Ok(format!(
        "procs\t{}\ncpu\t{cpu:.1}%\nrss\t{:.1} MiB\n",
        after.len(),
        rss as f64 / 1024.0
    ))
}

// ---------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;

    fn term(rows: usize, cols: usize, input: &str) -> Term {
        let mut t = Term::new(rows, cols);
        t.feed(input.as_bytes());
        t
    }

    #[test]
    fn text_and_newlines() {
        assert_eq!(term(3, 10, "ab\r\ncd").contents(), "ab\ncd");
    }

    #[test]
    fn autowrap_and_scroll() {
        assert_eq!(term(2, 3, "abcdefgh").contents(), "def\ngh");
    }

    #[test]
    fn cursor_position_and_erase() {
        let t = term(3, 5, "aaaaa\r\nbbbbb\r\nccccc\x1b[2;3H\x1b[K");
        assert_eq!(t.contents(), "aaaaa\nbb\nccccc");
        let t = term(3, 5, "aaaaa\r\nbbbbb\x1b[1;1H\x1b[2J");
        assert_eq!(t.contents(), "");
    }

    #[test]
    fn insert_delete_chars_and_lines() {
        assert_eq!(term(1, 6, "abcdef\x1b[1;2H\x1b[2P").contents(), "adef");
        assert_eq!(term(1, 6, "abcd\x1b[1;2H\x1b[2@").contents(), "a  bcd");
        assert_eq!(term(3, 3, "a\r\nb\r\nc\x1b[1;1H\x1b[L").contents(), "\na\nb");
        assert_eq!(term(3, 3, "a\r\nb\r\nc\x1b[1;1H\x1b[M").contents(), "b\nc");
    }

    #[test]
    fn scroll_region() {
        let t = term(4, 3, "1\r\n2\r\n3\r\n4\x1b[2;3r\x1b[3;1H\n\n");
        assert_eq!(t.contents(), "1\n\n\n4");
    }

    #[test]
    fn alternate_screen_restores_main() {
        let t = term(2, 5, "main\x1b[?1049hALT\x1b[?1049l");
        assert_eq!(t.contents(), "main");
    }

    #[test]
    fn wide_chars_and_utf8() {
        assert_eq!(term(1, 10, "日本x").contents(), "日本x");
        assert_eq!(term(1, 10, "héllo").contents(), "héllo");
        // overwrite the right half of a wide char: no orphaned half remains
        assert_eq!(term(1, 4, "日\x1b[1;2Hx").contents(), " x");
        // wide char that doesn't fit at the end of a line wraps
        assert_eq!(term(2, 3, "ab日").contents(), "ab\n日");
    }

    #[test]
    fn osc_and_colors_are_invisible() {
        assert_eq!(term(1, 10, "\x1b]0;title\x07\x1b[1;31mred\x1b[0m").contents(), "red");
        assert_eq!(term(1, 10, "\x1b]8;;http://x\x1b\\link").contents(), "link");
    }

    #[test]
    fn answers_queries() {
        let t = term(5, 10, "\x1b[3;4H\x1b[6n\x1b[c");
        assert_eq!(t.reply, b"\x1b[3;4R\x1b[?1;2c");
    }

    #[test]
    fn resize_keeps_content_and_clamps_cursor() {
        let mut t = term(3, 5, "abcde\r\nfghij\x1b[3;5H");
        t.resize(2, 3);
        assert_eq!(t.contents(), "abc\nfgh");
        assert_eq!((t.row, t.col), (1, 2));
    }

    #[test]
    fn unescape_works() {
        assert_eq!(unescape(r"a\r\x03\e\\").unwrap(), b"a\r\x03\x1b\\");
        assert!(unescape(r"\q").is_err());
        assert!(unescape(r"\x0").is_err());
        assert!(unescape("\\xé1").is_err());
    }

    /// The daemon runs with panic=abort, so the emulator must never panic on
    /// any input. Feed it a lot of random and escape-heavy garbage.
    #[test]
    fn fuzz_never_panics() {
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let pieces: &[&[u8]] = &[
            b"\x1b[", b"\x1b[?", b"\x1b]", b"\x1bP", b"\x1b", b";", b"9999999999999999999999",
            b"0", b"1", b"65535", b"\x07", b"\r", b"\n", b"\t", b"\x08", "日".as_bytes(), b"\xf0\x9f",
            b"\xff", b"A", b"H", b"J", b"K", b"L", b"M", b"P", b"@", b"X", b"S", b"T", b"r",
            b"h", b"l", b"1049", b"n", b"c", b"\x1b7", b"\x1b8", b"\x1bM", b"\x1bc", b"x",
        ];
        for round in 0..300 {
            let mut t = Term::new(1 + (next() % 30) as usize, 1 + (next() % 30) as usize);
            for _ in 0..3000 {
                if next() % 4 == 0 {
                    t.feed(&[next() as u8]);
                } else {
                    t.feed(pieces[(next() % pieces.len() as u64) as usize]);
                }
                if next() % 500 == 0 {
                    t.resize(1 + (next() % 30) as usize, 1 + (next() % 30) as usize);
                }
            }
            let _ = t.contents();
            assert!(t.row < t.rows && t.col < t.cols, "round {round}");
        }
    }
}
