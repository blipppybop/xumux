//! xumux — a lean local terminal multiplexer / PTY broker. Zero dependencies.
//!
//! `xumux serve` runs the daemon on a Unix socket (mode 0600).
//! Every other subcommand is a client that sends one line and prints the reply.
//!
//! Wire protocol: one request line in, `OK\n<body>` or `ERR <msg>\n` out, then
//! close. `attach` is the exception: after `OK\n` the connection becomes a raw
//! bidirectional stream (program output out, keystrokes in).

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
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
usage: xumux COMMAND [ARGS]

daemon
  serve                       run the daemon (foreground)
  shutdown                    kill every session and stop the daemon

sessions
  spawn NAME COMMAND...       start COMMAND (via sh -c) in a new 40x120 PTY
  list                        name, pid, status, command (tab-separated)
  info NAME                   pid, status, size, cursor, title, history, ...
  rename NAME NEW             rename a session
  kill NAME                   kill the session's whole process tree

input
  send NAME TEXT              write TEXT; escapes: \\r \\n \\t \\e \\\\ \\xHH
  keys NAME KEY...            tmux-style keys: Enter Tab Esc BSpace Up Down
                              Left Right Home End PgUp PgDn F1-F12 C-x M-x;
                              anything else is typed as text

output
  screen NAME                 the visible screen as text
  history NAME [LINES]        scrollback + screen (last LINES lines)
  wait NAME MS TEXT           block until TEXT is on screen (or MS elapse)
  log NAME PATH|off           append raw output to PATH (mode 0600)
  stats NAME                  CPU % and RSS of the session's process tree

terminal
  attach NAME                 interactive: type into the session; Ctrl-] detaches
  resize NAME ROWS COLS       resize the PTY (1-1000 each)

socket: $XUMUX_SOCK, else $XDG_RUNTIME_DIR/xumux.sock, else /tmp/xumux-$UID/xumux.sock
names:  1-64 chars of A-Z a-z 0-9 . _ -
cwd/env per session: xumux spawn api 'cd ~/api && PORT=8080 exec ./server'
";

const ROWS: u16 = 40;
const COLS: u16 = 120;
const MAX_DIM: u16 = 1000;
const MAX_REQUEST: u64 = 1 << 20;
const MAX_NAME: usize = 64;
const MAX_WAIT_MS: u64 = 600_000;
const INPUT_QUEUE: usize = 1024; // queued writes per session
const MAX_PENDING: usize = 4 << 20; // queued input bytes per session before `send` reports "full"
const IO_TIMEOUT: Duration = Duration::from_secs(5); // per client connection
const WATCH_TIMEOUT: Duration = Duration::from_secs(5); // a stuck attached client is cut off
const WATCH_QUEUE: usize = 1024; // output chunks an attached client may lag behind
const USER_HZ: f64 = 100.0; // /proc reports CPU time in USER_HZ, fixed at 100 on Linux
const DETACH_KEY: u8 = 0x1d; // Ctrl-]
/// Internal: the daemon starts programs as `/proc/self/exe __pty-exec CMD`.
const PTY_EXEC: &str = "__pty-exec";

struct Session {
    pid: u32,
    cmd: String,
    child: Mutex<Child>,
    /// Program input. A dedicated writer thread drains it, so neither the
    /// output reader nor a client ever blocks on a program that isn't reading.
    input: Input,
    master: File,
    /// Set once the reader has consumed the program's last byte of output.
    closed: Arc<AtomicBool>,
    /// Lock order: `out` before `term`.
    out: Arc<Mutex<Outputs>>,
    term: Arc<Mutex<Term>>,
}

/// Bounded program input: at most INPUT_QUEUE chunks and MAX_PENDING bytes.
#[derive(Clone)]
struct Input {
    tx: SyncSender<Vec<u8>>,
    pending: Arc<AtomicUsize>,
}

impl Input {
    fn push(&self, bytes: Vec<u8>) -> Result<(), String> {
        let len = bytes.len();
        if self.pending.fetch_add(len, Ordering::AcqRel) + len > MAX_PENDING {
            self.pending.fetch_sub(len, Ordering::AcqRel);
            return Err("input queue full: the program is not reading its input".into());
        }
        self.tx.try_send(bytes).map_err(|e| {
            self.pending.fetch_sub(len, Ordering::AcqRel);
            match e {
                TrySendError::Full(_) => "input queue full: the program is not reading its input".into(),
                TrySendError::Disconnected(_) => "session input is closed".into(),
            }
        })
    }

    /// For attached keyboards: wait for room instead of failing.
    fn push_wait(&self, bytes: Vec<u8>) -> Result<(), ()> {
        loop {
            match self.push(bytes.clone()) {
                Ok(()) => return Ok(()),
                Err(e) if e.contains("closed") => return Err(()),
                Err(_) => thread::sleep(Duration::from_millis(10)),
            }
        }
    }
}

#[derive(Default)]
struct Outputs {
    log: Option<(PathBuf, File)>,
    /// Attached clients. Each has its own sender thread and bounded queue, so
    /// a slow client is dropped instead of slowing the session down.
    watchers: Vec<(u64, SyncSender<Arc<[u8]>>)>,
}

type Sessions = Arc<Mutex<HashMap<String, Arc<Session>>>>;

static SOCKET: OnceLock<PathBuf> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None | Some("-h" | "--help" | "help") => {
            eprint!("{USAGE}");
            std::process::exit(2)
        }
        Some("serve") => serve(),
        Some(PTY_EXEC) => pty_exec(&args),
        Some("attach") => attach(&args),
        _ => client(&args),
    }
}

/// `$XUMUX_SOCK` is used as given (the caller owns that choice). The default
/// locations must be a real directory owned by us and closed to everyone else,
/// so nobody can pre-create or swap the socket.
fn sock_path() -> PathBuf {
    if let Ok(p) = env::var("XUMUX_SOCK") {
        return p.into();
    }
    let dir = match env::var("XDG_RUNTIME_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => {
            let d = PathBuf::from(format!("/tmp/xumux-{}", sys::uid()));
            let _ = fs::DirBuilder::new().mode(0o700).create(&d);
            d
        }
    };
    if let Err(e) = check_private_dir(&dir) {
        die(format!("refusing socket dir {}: {e}", dir.display()));
    }
    dir.join("xumux.sock")
}

fn check_private_dir(dir: &Path) -> Result<(), String> {
    let m = fs::symlink_metadata(dir).map_err(err)?;
    if !m.is_dir() {
        return Err("not a directory (symlinks are not followed)".into());
    }
    if m.uid() != sys::uid() {
        return Err(format!("owned by uid {}, not you", m.uid()));
    }
    if m.mode() & 0o077 != 0 {
        return Err(format!("mode {:o} is open to group/others, want 700", m.mode() & 0o777));
    }
    Ok(())
}

fn die(msg: impl std::fmt::Display) -> ! {
    eprintln!("xumux: {msg}");
    std::process::exit(1)
}

fn connect() -> UnixStream {
    let path = sock_path();
    UnixStream::connect(&path)
        .unwrap_or_else(|e| die(format!("{}: {e} (is `xumux serve` running?)", path.display())))
}

// ---------------------------------------------------------------- client

fn client(args: &[String]) {
    let mut args = args.to_vec();
    // The daemon's cwd is not ours: make log paths absolute here.
    if args[0] == "log" && args.len() == 3 && args[2] != "off" {
        let cwd = env::current_dir().unwrap_or_else(|e| die(e));
        args[2] = cwd.join(&args[2]).to_string_lossy().into_owned();
    }
    let mut stream = connect();
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

fn attach(args: &[String]) {
    let [_, name] = args else { die("usage: attach NAME") };
    let mut stream = connect();
    let (rows, cols) = sys::term_size(&io::stdout()).unwrap_or((0, 0));
    stream
        .write_all(format!("attach {name} {rows} {cols}\n").as_bytes())
        .unwrap_or_else(|e| die(e));
    let mut status = Vec::new();
    let mut byte = [0u8];
    while stream.read(&mut byte).unwrap_or(0) == 1 && byte[0] != b'\n' {
        status.push(byte[0]);
    }
    let status = String::from_utf8_lossy(&status);
    if status != "OK" {
        die(status.strip_prefix("ERR ").unwrap_or(&status));
    }
    eprint!("[xumux: attached to {name}; Ctrl-] detaches]\r\n");
    let raw = sys::RawMode::enter(&io::stdin()); // restored on drop
    let mut to_server = stream.try_clone().unwrap_or_else(|e| die(e));
    thread::spawn(move || {
        let mut buf = [0u8; 4096];
        let mut stdin = io::stdin().lock();
        while let Ok(n @ 1..) = stdin.read(&mut buf) {
            let chunk = &buf[..n];
            if let Some(i) = chunk.iter().position(|&b| b == DETACH_KEY) {
                // The daemon sees the detach key and lets go; we stop reading.
                let _ = to_server.write_all(&chunk[..=i]);
                let _ = to_server.shutdown(Shutdown::Both);
                return;
            }
            if to_server.write_all(chunk).is_err() {
                return;
            }
        }
        // stdin ended (e.g. piped input): keep showing output until the session ends.
        let _ = to_server.shutdown(Shutdown::Write);
    });
    let mut stdout = io::stdout().lock();
    let mut buf = [0u8; 16384];
    while let Ok(n @ 1..) = stream.read(&mut buf) {
        if stdout.write_all(&buf[..n]).and_then(|_| stdout.flush()).is_err() {
            break;
        }
    }
    drop(raw);
    let _ = stdout.write_all(b"\x1b[0m\r\n[xumux: detached]\r\n");
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
    let _ = SOCKET.set(path);

    let sessions: Sessions = Default::default();
    for conn in listener.incoming() {
        let Ok(stream) = conn else { continue };
        let sessions = sessions.clone();
        thread::spawn(move || handle(stream, &sessions));
    }
}

fn handle(stream: UnixStream, sessions: &Sessions) {
    // A client that connects and goes quiet must not pin a thread forever.
    let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
    // Defense in depth on top of the 0600 socket: only our own uid may talk to us.
    let request = match sys::peer_uid(&stream) {
        Ok(uid) if uid == sys::uid() => read_request(&stream),
        Ok(uid) => Err(format!("permission denied for uid {uid}")),
        Err(e) => Err(err(e)),
    };
    let (cmd, rest) = match &request {
        Ok((line, _)) => split1(line),
        Err(_) => ("", ""),
    };
    let result = match &request {
        Err(e) => Err(e.clone()),
        Ok((_, extra)) if cmd == "attach" => match serve_attach(&stream, rest, extra, sessions) {
            Ok(()) => return,
            Err(e) => Err(e),
        },
        Ok((line, _)) => dispatch(line, sessions),
    };
    let reply = match result {
        Ok(body) => format!("OK\n{body}"),
        Err(e) => format!("ERR {e}\n"),
    };
    let _ = (&stream).write_all(reply.as_bytes());
    if request.is_err() {
        // Closing with unread input makes Linux reset the connection, which
        // would destroy the ERR reply before the client reads it. Drain briefly.
        let _ = stream.shutdown(Shutdown::Write);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
        let _ = io::copy(&mut (&stream).take(16 << 20), &mut io::sink());
    }
    if cmd == "shutdown" && reply.starts_with("OK") {
        if let Some(p) = SOCKET.get() {
            let _ = fs::remove_file(p);
        }
        std::process::exit(0);
    }
}

/// Read one request line. Returns it plus any bytes that arrived after the
/// newline (keystrokes typed right after `attach`). One deadline covers the
/// whole request, so dribbling a byte at a time cannot hold a thread.
fn read_request(stream: &UnixStream) -> Result<(String, Vec<u8>), String> {
    let deadline = Instant::now() + IO_TIMEOUT;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let end = loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err("timed out".into());
        }
        stream.set_read_timeout(Some(left)).map_err(err)?;
        let n = match (&*stream).read(&mut chunk) {
            Ok(0) => return Err("incomplete request (no newline)".into()),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                return Err("timed out".into());
            }
            Err(e) => return Err(err(e)),
        };
        let start = buf.len();
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf[start..].iter().position(|&b| b == b'\n') {
            break start + i;
        }
        if buf.len() as u64 > MAX_REQUEST {
            return Err(format!("request exceeds {MAX_REQUEST} bytes"));
        }
    };
    if end as u64 > MAX_REQUEST {
        return Err(format!("request exceeds {MAX_REQUEST} bytes"));
    }
    let extra = buf.split_off(end + 1);
    buf.truncate(end);
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    let line = String::from_utf8(buf).map_err(|_| "request is not valid UTF-8".to_string())?;
    Ok((line, extra))
}

fn valid_name(name: &str) -> Result<(), String> {
    let ok = (1..=MAX_NAME).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid name '{}': use 1-{MAX_NAME} chars of A-Z a-z 0-9 . _ -",
            name.escape_debug()
        ))
    }
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

fn status(s: &Session) -> String {
    match s.child.lock().unwrap().try_wait() {
        Ok(None) => "running".into(),
        Ok(Some(st)) => match (st.code(), st.signal()) {
            (Some(c), _) => format!("exited({c})"),
            (_, Some(sig)) => format!("killed(sig {sig})"),
            _ => "exited".into(),
        },
        Err(_) => "unknown".into(),
    }
}

fn queue_input(s: &Session, bytes: Vec<u8>) -> Result<String, String> {
    s.input.push(bytes).map(|()| String::new())
}

fn resize_session(s: &Session, rows: u16, cols: u16) -> Result<(), String> {
    sys::set_size(&s.master, rows, cols).map_err(err)?;
    s.term.lock().unwrap().resize(rows.into(), cols.into());
    Ok(())
}

/// Take down everything the program started, not just the program: its
/// descendants, plus anything still in its session (orphaned background jobs
/// get reparented but keep the session id).
fn kill_session(s: &Session) {
    let mut child = s.child.lock().unwrap();
    let alive = matches!(child.try_wait(), Ok(None));
    let all = procs();
    let mut doomed: Vec<u32> = all.iter().filter(|p| p.sid == s.pid).map(|p| p.pid).collect();
    if alive {
        doomed.extend(tree_of(&all, s.pid).into_keys());
    }
    for pid in doomed {
        if pid != s.pid {
            sys::kill_pid(pid);
        }
    }
    if alive {
        let _ = child.kill();
        let _ = child.wait();
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
            valid_name(name)?;
            spawn(name, command, sessions)
        }
        "send" => {
            let (name, text) = split1(rest);
            let s = get(sessions, name)?;
            queue_input(&s, unescape(text)?)
        }
        "keys" => {
            let (name, keys) = split1(rest);
            let s = get(sessions, name)?;
            queue_input(&s, parse_keys(keys))
        }
        "screen" => {
            let s = get(sessions, rest.trim())?;
            let contents = s.term.lock().unwrap().contents();
            Ok(format!("{contents}\n"))
        }
        "history" => {
            let mut it = rest.split_whitespace();
            let s = get(sessions, it.next().unwrap_or(""))?;
            let lines = match it.next() {
                Some(n) => Some(n.parse::<usize>().map_err(|_| format!("bad line count '{n}'"))?),
                None => None,
            };
            let text = s.term.lock().unwrap().history_text(lines);
            Ok(format!("{text}\n"))
        }
        "wait" => {
            let (name, rest) = split1(rest);
            let (ms, text) = split1(rest);
            let ms: u64 = ms.parse().map_err(|_| "usage: wait NAME MS TEXT")?;
            if text.is_empty() {
                return Err("usage: wait NAME MS TEXT".into());
            }
            let s = get(sessions, name)?;
            let deadline = Instant::now() + Duration::from_millis(ms.min(MAX_WAIT_MS));
            loop {
                if s.term.lock().unwrap().contents().contains(text) {
                    return Ok(String::new());
                }
                // `closed`, not the exit status: output can still be in flight
                // when the process exits.
                if s.closed.load(Ordering::Acquire) {
                    return Err(format!("session exited before '{text}' appeared"));
                }
                if Instant::now() >= deadline {
                    return Err(format!("timed out after {ms} ms waiting for '{text}'"));
                }
                thread::sleep(Duration::from_millis(10));
            }
        }
        "list" => {
            let map = sessions.lock().unwrap();
            let mut names: Vec<_> = map.keys().collect();
            names.sort();
            let mut out = String::new();
            for n in names {
                let s = &map[n];
                out += &format!("{n}\t{}\t{}\t{}\n", s.pid, status(s), s.cmd);
            }
            Ok(out)
        }
        "info" => {
            let s = get(sessions, rest.trim())?;
            let o = s.out.lock().unwrap();
            let t = s.term.lock().unwrap();
            let log = o.log.as_ref().map_or("-".into(), |(p, _)| p.display().to_string());
            Ok(format!(
                "pid\t{}\nstatus\t{}\nsize\t{}x{}\ncursor\t{},{}\nalt_screen\t{}\ntitle\t{}\nhistory\t{}\nattached\t{}\nlog\t{log}\ncommand\t{}\n",
                s.pid,
                status(&s),
                t.rows,
                t.cols,
                t.row + 1,
                t.col + 1,
                if t.main.is_some() { "yes" } else { "no" },
                t.title,
                t.history.len(),
                o.watchers.len(),
                s.cmd,
            ))
        }
        "log" => {
            let (name, path) = split1(rest);
            let s = get(sessions, name)?;
            if path == "off" {
                s.out.lock().unwrap().log = None;
                return Ok(String::new());
            }
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                return Err("log path must be absolute".into());
            }
            // O_NONBLOCK: opening a FIFO nobody reads must fail, not hang.
            let f = OpenOptions::new()
                .append(true)
                .create(true)
                .mode(0o600)
                .custom_flags(sys::O_NONBLOCK)
                .open(&path)
                .map_err(|e| format!("{}: {e}", path.display()))?;
            if !f.metadata().map_err(err)?.is_file() {
                return Err(format!("{}: not a regular file", path.display()));
            }
            s.out.lock().unwrap().log = Some((path, f));
            Ok(String::new())
        }
        "stats" => stats(get(sessions, rest.trim())?.pid),
        "resize" => {
            let mut it = rest.split_whitespace();
            let (Some(name), Some(r), Some(c)) = (it.next(), it.next(), it.next()) else {
                return Err("usage: resize NAME ROWS COLS".into());
            };
            let (rows, cols) = (dim(r)?, dim(c)?);
            let s = get(sessions, name)?;
            resize_session(&s, rows, cols)?;
            Ok(String::new())
        }
        "rename" => {
            let mut it = rest.split_whitespace();
            let (Some(old), Some(new)) = (it.next(), it.next()) else {
                return Err("usage: rename NAME NEW".into());
            };
            valid_name(new)?;
            let mut map = sessions.lock().unwrap();
            if map.contains_key(new) {
                return Err(format!("session '{new}' already exists"));
            }
            let s = map.remove(old).ok_or_else(|| format!("no session '{old}'"))?;
            map.insert(new.to_string(), s);
            Ok(String::new())
        }
        "kill" => {
            let name = rest.trim();
            let s = sessions
                .lock()
                .unwrap()
                .remove(name)
                .ok_or_else(|| format!("no session '{name}'"))?;
            kill_session(&s);
            Ok(String::new())
        }
        "shutdown" => {
            let all: Vec<_> = sessions.lock().unwrap().drain().map(|(_, s)| s).collect();
            for s in &all {
                kill_session(s);
            }
            Ok(format!("killed {} sessions\n", all.len()))
        }
        "" => Err("empty request".into()),
        other => Err(format!("unknown command '{other}' (try `xumux help`)")),
    }
}

fn serve_attach(stream: &UnixStream, rest: &str, typed: &[u8], sessions: &Sessions) -> Result<(), String> {
    let mut it = rest.split_whitespace();
    let s = get(sessions, it.next().unwrap_or(""))?;
    // The client sends its own size; "0 0" means unknown (not a terminal).
    if let (Some(Ok(r)), Some(Ok(c))) = (it.next().map(dim), it.next().map(dim)) {
        resize_session(&s, r, c)?;
    }
    let mut out = stream.try_clone().map_err(err)?;
    out.set_write_timeout(Some(WATCH_TIMEOUT)).map_err(err)?;
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = sync_channel::<Arc<[u8]>>(WATCH_QUEUE);
    thread::spawn(move || {
        for chunk in rx {
            if out.write_all(&chunk).is_err() {
                break;
            }
        }
        let _ = out.shutdown(Shutdown::Both); // dropped, detached or session over
    });
    {
        // Holding `out` keeps the reader from sending output between the
        // snapshot and registration, so the client sees every byte exactly once.
        let mut o = s.out.lock().unwrap();
        let mut first = b"OK\n".to_vec();
        first.extend_from_slice(s.term.lock().unwrap().redraw().as_bytes());
        tx.try_send(first.into()).map_err(err)?;
        o.watchers.push((id, tx));
    }
    let _ = stream.set_read_timeout(None);
    // Ctrl-] (DETACH_KEY) detaches. Plain end of input only means the client
    // has nothing more to type: it keeps receiving output until the session
    // ends or a write to it fails.
    let mut buf = [0u8; 4096];
    let mut pending = typed.to_vec();
    loop {
        let detach = pending.iter().position(|&b| b == DETACH_KEY);
        let keys = &pending[..detach.unwrap_or(pending.len())];
        if !keys.is_empty() && s.input.push_wait(keys.to_vec()).is_err() {
            break;
        }
        if detach.is_some() {
            s.out.lock().unwrap().watchers.retain(|(w, _)| *w != id);
            let _ = stream.shutdown(Shutdown::Both);
            break;
        }
        match (&*stream).read(&mut buf) {
            Ok(n @ 1..) => pending = buf[..n].to_vec(),
            _ => break,
        }
    }
    Ok(())
}

/// Runs in the new child, whose stdio is already the PTY: start a session,
/// take the PTY as controlling terminal, become `sh -c 'exec COMMAND'`.
fn pty_exec(args: &[String]) -> ! {
    let [_, command] = args else { die("internal command") };
    if let Err(e) = sys::make_controlling_tty() {
        die(format!("cannot take the terminal: {e}"));
    }
    let e = Command::new("/bin/sh").arg("-c").arg(format!("exec {command}")).exec();
    die(format!("/bin/sh: {e}"))
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

    // No pre_exec hook, so std can use posix_spawn (CLONE_VM) instead of
    // fork(): fork has to copy the daemon's memory map, which grows with every
    // session. The helper takes the terminal, then execs the shell.
    let mut cmd = Command::new("/proc/self/exe");
    cmd.arg(PTY_EXEC)
        .arg(command)
        .env("TERM", "xterm-256color")
        .stdin(Stdio::from(slave.try_clone().map_err(err)?))
        .stdout(Stdio::from(slave.try_clone().map_err(err)?))
        .stderr(Stdio::from(slave));
    if let Ok(home) = env::var("HOME") {
        cmd.current_dir(home);
    }
    let mut child = cmd.spawn().map_err(err)?;
    drop(cmd); // close our copies of the slave so reads hit EOF/EIO when the child exits

    // The master is non-blocking and both threads wait in poll(). A blocking
    // write to a PTY whose program is gone is never woken by Linux, so the
    // writer must be able to notice `closed` and give up.
    sys::set_nonblocking(&master).map_err(err)?;
    let closed = Arc::new(AtomicBool::new(false));

    // Writer thread: the only place that waits on the program's input.
    let (tx, queue) = sync_channel::<Vec<u8>>(INPUT_QUEUE);
    let input = Input { tx, pending: Default::default() };
    let mut writer = master.try_clone().map_err(err)?;
    let (gone, pending) = (closed.clone(), input.pending.clone());
    thread::spawn(move || {
        'chunks: loop {
            let chunk = match queue.recv_timeout(Duration::from_millis(100)) {
                Ok(chunk) => chunk,
                Err(RecvTimeoutError::Timeout) if gone.load(Ordering::Acquire) => break,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            };
            pending.fetch_sub(chunk.len(), Ordering::AcqRel);
            let mut rest = &chunk[..];
            while !rest.is_empty() {
                match writer.write(rest) {
                    Ok(n) => rest = &rest[n..],
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        if gone.load(Ordering::Acquire) {
                            break 'chunks;
                        }
                        sys::wait_ready(&writer, sys::POLLOUT, 100);
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => break 'chunks,
                }
            }
        }
    });

    // Reader thread: output -> emulator, log file, attached clients.
    let mut reader = master.try_clone().map_err(err)?;
    let term = Arc::new(Mutex::new(Term::new(ROWS.into(), COLS.into())));
    let out: Arc<Mutex<Outputs>> = Default::default();
    let (feed, fanout, answer, done) = (term.clone(), out.clone(), input.clone(), closed.clone());
    thread::spawn(move || {
        let mut buf = [0u8; 16384];
        loop {
            let n = match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    sys::wait_ready(&reader, sys::POLLIN, -1);
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break, // EIO once the child side is closed
            };
            let chunk = &buf[..n];
            let mut o = fanout.lock().unwrap();
            let reply = {
                let mut t = feed.lock().unwrap();
                t.feed(chunk);
                std::mem::take(&mut t.reply)
            };
            if !reply.is_empty() {
                let _ = answer.push(reply); // dropped if the program isn't reading
            }
            if let Some((_, f)) = &mut o.log
                && f.write_all(chunk).is_err()
            {
                o.log = None;
            }
            if !o.watchers.is_empty() {
                let shared: Arc<[u8]> = chunk.into();
                // a client too far behind is dropped; it can re-attach for a fresh redraw
                o.watchers.retain(|(_, w)| w.try_send(shared.clone()).is_ok());
            }
        }
        done.store(true, Ordering::Release);
        drop(answer);
        fanout.lock().unwrap().watchers.clear(); // senders flush, then close: clients see EOF
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
            input,
            master,
            closed,
            out,
            term,
        }),
    );
    Ok(format!("{pid}\n"))
}

/// tmux `send-keys` style: each word is a key name if it is one, otherwise
/// literal text. Consecutive literal words keep a single space between them.
fn parse_keys(keys: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut last_literal = false;
    for word in keys.split(' ').filter(|w| !w.is_empty()) {
        match key(word) {
            Some(bytes) => {
                out.extend_from_slice(&bytes);
                last_literal = false;
            }
            None => {
                if last_literal {
                    out.push(b' ');
                }
                out.extend_from_slice(word.as_bytes());
                last_literal = true;
            }
        }
    }
    out
}

fn key(name: &str) -> Option<Vec<u8>> {
    let seq: &[u8] = match name {
        "Enter" => b"\r",
        "Tab" => b"\t",
        "BTab" => b"\x1b[Z",
        "Space" => b" ",
        "Esc" | "Escape" => b"\x1b",
        "BSpace" => b"\x7f",
        "Up" => b"\x1b[A",
        "Down" => b"\x1b[B",
        "Right" => b"\x1b[C",
        "Left" => b"\x1b[D",
        "Home" => b"\x1b[H",
        "End" => b"\x1b[F",
        "PgUp" | "PageUp" => b"\x1b[5~",
        "PgDn" | "PageDown" => b"\x1b[6~",
        "IC" | "Insert" => b"\x1b[2~",
        "DC" | "Delete" => b"\x1b[3~",
        "F1" => b"\x1bOP",
        "F2" => b"\x1bOQ",
        "F3" => b"\x1bOR",
        "F4" => b"\x1bOS",
        "F5" => b"\x1b[15~",
        "F6" => b"\x1b[17~",
        "F7" => b"\x1b[18~",
        "F8" => b"\x1b[19~",
        "F9" => b"\x1b[20~",
        "F10" => b"\x1b[21~",
        "F11" => b"\x1b[23~",
        "F12" => b"\x1b[24~",
        _ => {
            if let Some(rest) = name.strip_prefix("M-").filter(|r| !r.is_empty()) {
                let mut v = vec![0x1b];
                v.extend(key(rest).unwrap_or_else(|| rest.as_bytes().to_vec()));
                return Some(v);
            }
            let c = name.strip_prefix("C-")?;
            let [b] = c.as_bytes() else { return None };
            return Some(vec![match b {
                b'a'..=b'z' => b - b'a' + 1,
                b'A'..=b'Z' => b - b'A' + 1,
                b'@' | b' ' | b'2' => 0,
                b'[' | b'3' => 0x1b,
                b'\\' | b'4' => 0x1c,
                b']' | b'5' => 0x1d,
                b'^' | b'6' => 0x1e,
                b'_' | b'7' => 0x1f,
                b'?' | b'8' => 0x7f,
                _ => return None,
            }]);
        }
    };
    Some(seq.to_vec())
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
// The only `unsafe` in xumux. Ten libc symbols, already linked by std, and constants from
// <asm-generic/ioctls.h> and <asm-generic/socket.h>.

mod sys {
    use std::ffi::{c_int, c_ulong, c_void};
    use std::io;
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
        fn setsid() -> c_int;
        fn getuid() -> u32;
        fn kill(pid: c_int, sig: c_int) -> c_int;
        fn getsockopt(fd: c_int, level: c_int, name: c_int, val: *mut c_void, len: *mut u32) -> c_int;
        fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
        fn poll(fds: *mut PollFd, nfds: u64, timeout_ms: c_int) -> c_int;
        fn tcgetattr(fd: c_int, termios: *mut c_void) -> c_int;
        fn tcsetattr(fd: c_int, action: c_int, termios: *const c_void) -> c_int;
        fn cfmakeraw(termios: *mut c_void);
    }

    /// `struct termios` is treated as an opaque blob (it is 60 bytes on Linux);
    /// we only ever hand it back to libc, so its layout never matters here.
    type Termios = [u64; 16];
    const TCSANOW: c_int = 0;

    /// Puts a terminal in raw mode; the original settings come back on drop.
    pub struct RawMode {
        fd: c_int,
        saved: Termios,
    }

    impl RawMode {
        pub fn enter(tty: &impl AsRawFd) -> Option<RawMode> {
            let fd = tty.as_raw_fd();
            let mut saved: Termios = [0; 16];
            // SAFETY: the buffer is larger and more aligned than struct termios.
            unsafe {
                if tcgetattr(fd, saved.as_mut_ptr().cast()) != 0 {
                    return None; // not a terminal
                }
                let mut raw = saved;
                cfmakeraw(raw.as_mut_ptr().cast());
                if tcsetattr(fd, TCSANOW, raw.as_ptr().cast()) != 0 {
                    return None;
                }
            }
            Some(RawMode { fd, saved })
        }
    }

    impl Drop for RawMode {
        fn drop(&mut self) {
            // SAFETY: restores the settings tcgetattr filled in.
            unsafe { tcsetattr(self.fd, TCSANOW, self.saved.as_ptr().cast()) };
        }
    }

    /// (rows, cols) of a terminal.
    pub fn term_size(tty: &impl AsRawFd) -> Option<(u16, u16)> {
        let mut ws = Winsize { rows: 0, cols: 0, xpixel: 0, ypixel: 0 };
        // SAFETY: valid fd; pointer to a live winsize.
        let r = unsafe { ioctl(tty.as_raw_fd(), TIOCGWINSZ, &mut ws as *mut Winsize) };
        (r == 0 && ws.rows > 0 && ws.cols > 0).then_some((ws.rows, ws.cols))
    }

    const F_GETFL: c_int = 3;
    const F_SETFL: c_int = 4;
    pub const O_NONBLOCK: c_int = 0o4000;
    pub const POLLIN: i16 = 0x1;
    pub const POLLOUT: i16 = 0x4;

    #[repr(C)]
    pub struct PollFd {
        fd: c_int,
        events: i16,
        revents: i16,
    }

    pub fn set_nonblocking(f: &impl AsRawFd) -> io::Result<()> {
        // SAFETY: valid fd; integer-only fcntl commands.
        unsafe {
            let flags = fcntl(f.as_raw_fd(), F_GETFL);
            check(flags)?;
            check(fcntl(f.as_raw_fd(), F_SETFL, flags | O_NONBLOCK))
        }
    }

    /// Wait until `f` is ready for `events` (or hung up), up to `timeout_ms`
    /// (-1 = forever). EINTR counts as a spurious wakeup.
    pub fn wait_ready(f: &impl AsRawFd, events: i16, timeout_ms: c_int) {
        let mut p = PollFd { fd: f.as_raw_fd(), events, revents: 0 };
        // SAFETY: one live, correctly laid out pollfd.
        unsafe { poll(&mut p, 1, timeout_ms) };
    }

    const SIGKILL: c_int = 9;
    const SOL_SOCKET: c_int = 1;
    const SO_PEERCRED: c_int = 17;

    #[repr(C)]
    struct Ucred {
        pid: i32,
        uid: u32,
        gid: u32,
    }

    pub fn uid() -> u32 {
        // SAFETY: getuid takes no arguments and cannot fail.
        unsafe { getuid() }
    }

    /// SIGKILL one process. Refuses pid 0, 1 and anything that would wrap to a
    /// negative pid, since kill(0|-1|-n) target whole process groups.
    pub fn kill_pid(pid: u32) {
        if let Ok(p @ 2..) = c_int::try_from(pid) {
            // SAFETY: plain syscall with a positive pid.
            unsafe { kill(p, SIGKILL) };
        }
    }

    /// The uid of the process on the other end of a Unix socket (SO_PEERCRED).
    pub fn peer_uid(sock: &impl AsRawFd) -> io::Result<u32> {
        let mut cred = Ucred { pid: 0, uid: u32::MAX, gid: u32::MAX };
        let mut len = std::mem::size_of::<Ucred>() as u32;
        // SAFETY: valid fd; cred/len point to live locals and len holds cred's size.
        check(unsafe {
            getsockopt(sock.as_raw_fd(), SOL_SOCKET, SO_PEERCRED, (&mut cred as *mut Ucred).cast(), &mut len)
        })?;
        Ok(cred.uid)
    }

    pub const O_NOCTTY: i32 = 0o400;
    const TIOCSCTTY: c_ulong = 0x540E;
    const TIOCGWINSZ: c_ulong = 0x5413;
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

    /// Start a new session and adopt stdin (a PTY slave) as the controlling terminal.
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
    history: VecDeque<String>, // lines scrolled off the top of the main screen
    title: String,             // set by OSC 0 / OSC 2
    osc: Vec<u8>,
}

const HISTORY_LINES: usize = 10_000;
const MAX_OSC: usize = 1024;

fn width(c: char) -> usize {
    match c as u32 {
        // C0/C1 controls never reach the grid: U+009B is CSI on some terminals,
        // so letting it through would let a program inject escapes into
        // whoever prints `screen`.
        0x00..=0x1F | 0x7F..=0x9F => 0,
        // combining marks, zero-width chars, and bidi controls (U+202A-202E,
        // U+2066-2069: "Trojan Source" reordering) are dropped too
        0x300..=0x36F
        | 0x200B..=0x200F
        | 0x2028..=0x202E
        | 0x2060..=0x206F
        | 0x20D0..=0x20FF
        | 0xFE00..=0xFE0F
        | 0xFEFF => 0,
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
            history: VecDeque::new(),
            title: String::new(),
            osc: Vec::new(),
        }
    }

    fn line(&self, r: usize) -> String {
        let s: String = self.grid[r * self.cols..(r + 1) * self.cols]
            .iter()
            .filter(|&&c| c != WIDE_TAIL)
            .collect();
        s.trim_end_matches(' ').to_string()
    }

    /// Scrollback followed by the screen; the last `lines` lines if given.
    fn history_text(&self, lines: Option<usize>) -> String {
        let screen = self.contents();
        let all: Vec<&str> = self
            .history
            .iter()
            .map(String::as_str)
            .chain(screen.lines())
            .collect();
        let skip = lines.map_or(0, |n| all.len().saturating_sub(n));
        all[skip..].join("\n")
    }

    /// Escape sequence that repaints this screen on an attaching terminal.
    fn redraw(&self) -> String {
        let mut out = String::from("\x1b[0m\x1b[H\x1b[2J");
        for r in 0..self.rows {
            out += &format!("\x1b[{};1H{}", r + 1, self.line(r));
        }
        out += &format!("\x1b[{};{}H", self.row + 1, self.col + 1);
        out
    }

    fn osc_done(&mut self) {
        let payload = String::from_utf8_lossy(&self.osc).into_owned();
        self.osc.clear();
        if let Some(t) = payload.strip_prefix("0;").or_else(|| payload.strip_prefix("2;")) {
            self.title = t.chars().filter(|&c| width(c) > 0).take(256).collect();
        }
    }

    fn contents(&self) -> String {
        let mut out = String::new();
        for r in 0..self.rows {
            out += &self.line(r);
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
                0x07 => {
                    self.osc_done();
                    self.state = Ground;
                }
                0x1b => {
                    self.osc_done();
                    self.state = OscEsc;
                }
                _ => {
                    if self.osc.len() < MAX_OSC {
                        self.osc.push(b);
                    }
                }
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
            b']' => {
                self.osc.clear();
                self.state = State::Osc;
            }
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
            b'c' => {
                let history = std::mem::take(&mut self.history);
                *self = Term::new(self.rows, self.cols);
                self.history = history;
            }
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
        if t == 0 && self.main.is_none() {
            for r in 0..n {
                let line = self.line(r);
                self.history.push_back(line);
            }
            while self.history.len() > HISTORY_LINES {
                self.history.pop_front();
            }
        }
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

struct Proc {
    pid: u32,
    ppid: u32,
    sid: u32,
    ticks: u64, // utime + stime
}

/// One pass over /proc.
fn procs() -> Vec<Proc> {
    let mut out = Vec::new();
    for entry in fs::read_dir("/proc").into_iter().flatten().flatten() {
        let Some(pid) = entry.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        let Ok(s) = fs::read_to_string(format!("/proc/{pid}/stat")) else { continue };
        // comm may contain spaces/parens, so parse after the last ')'
        let Some(close) = s.rfind(')') else { continue };
        let f: Vec<&str> = s[close + 1..].split_whitespace().collect();
        let num = |i: usize| f.get(i).and_then(|v| v.parse::<u64>().ok());
        if let (Some(ppid), Some(sid), Some(u), Some(st)) = (num(1), num(3), num(11), num(12)) {
            out.push(Proc { pid, ppid: ppid as u32, sid: sid as u32, ticks: u + st });
        }
    }
    out
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

/// `root` and all its descendants, with their CPU ticks.
fn tree_of(all: &[Proc], root: u32) -> HashMap<u32, u64> {
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    let ticks: HashMap<u32, u64> = all.iter().map(|p| (p.pid, p.ticks)).collect();
    for p in all {
        children.entry(p.ppid).or_default().push(p.pid);
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

fn tree(root: u32) -> HashMap<u32, u64> {
    tree_of(&procs(), root)
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
    fn controls_and_bidi_never_reach_the_screen() {
        // U+009B (C1 CSI), U+202E (RTL override), U+2066 (LRI), U+FEFF, raw DEL
        let t = term(1, 20, "a\u{9b}31mb\u{202e}c\u{2066}d\u{feff}e\x7ff");
        let out = t.contents();
        assert_eq!(out, "a31mbcdef");
        assert!(out.chars().all(|c| !c.is_control()));
    }

    #[test]
    fn names_are_validated() {
        for ok in ["a", "grok", "job-1.2_x", &"x".repeat(64)] {
            assert!(valid_name(ok).is_ok(), "{ok}");
        }
        for bad in ["", "a b", "a\tb", "../x", "a/b", "é", &"x".repeat(65)] {
            assert!(valid_name(bad).is_err(), "{bad}");
        }
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
