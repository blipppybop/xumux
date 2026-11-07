//! xumux — a lean local terminal multiplexer / PTY broker.
//!
//! `xumux serve` runs the daemon on a Unix socket (mode 0600).
//! Every other subcommand is a client that sends one line and prints the reply.
//!
//! Wire protocol: one request line in, `OK\n<body>` or `ERR <msg>\n` out, then close.

use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::num::NonZeroU16;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{env, fs, thread};

const USAGE: &str = "\
usage:
  xumux serve                      run the daemon (foreground)
  xumux spawn NAME COMMAND...      start COMMAND (via sh -c) in a new PTY
  xumux send NAME TEXT             write TEXT; escapes: \\r \\n \\t \\e \\\\ \\xHH
  xumux screen NAME                print the current screen
  xumux list                       list sessions: name, pid, status, command
  xumux stats NAME                 CPU % and RSS of the session's process tree
  xumux resize NAME ROWS COLS      resize the PTY
  xumux kill NAME                  kill the session and forget it

socket: $XUMUX_SOCK, else $XDG_RUNTIME_DIR/xumux.sock
tip: most TUIs submit on carriage return:  xumux send grok 'hello\\r'
";

const ROWS: u16 = 40;
const COLS: u16 = 120;
const MAX_REQUEST: u64 = 1 << 20;
const USER_HZ: f64 = 100.0; // /proc reports CPU time in USER_HZ, fixed at 100 on Linux

struct Session {
    pid: Option<u32>,
    cmd: String,
    child: Mutex<Box<dyn Child + Send + Sync>>,
    writer: Mutex<Box<dyn Write + Send>>,
    master: Mutex<Box<dyn MasterPty + Send>>,
    parser: Arc<Mutex<vt100::Parser>>,
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
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
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
            let contents = s.parser.lock().unwrap().screen().contents();
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
                    Ok(Some(st)) => format!("exited({})", st.exit_code()),
                    Ok(None) => "running".into(),
                    Err(_) => "unknown".into(),
                };
                let pid = s.pid.map_or("-".into(), |p| p.to_string());
                out += &format!("{n}\t{pid}\t{status}\t{}\n", s.cmd);
            }
            Ok(out)
        }
        "stats" => {
            let s = get(sessions, rest.trim())?;
            let pid = s.pid.ok_or("pid unknown")?;
            stats(pid)
        }
        "resize" => {
            let mut it = rest.split_whitespace();
            let (Some(name), Some(r), Some(c)) = (it.next(), it.next(), it.next()) else {
                return Err("usage: resize NAME ROWS COLS".into());
            };
            let rows: NonZeroU16 = r.parse().map_err(err)?;
            let cols: NonZeroU16 = c.parse().map_err(err)?;
            let s = get(sessions, name)?;
            s.master
                .lock()
                .unwrap()
                .resize(size(rows.get(), cols.get()))
                .map_err(err)?;
            s.parser.lock().unwrap().set_size(rows, cols);
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

fn size(rows: u16, cols: u16) -> PtySize {
    PtySize { rows, cols, pixel_width: 0, pixel_height: 0 }
}

fn spawn(name: &str, command: &str, sessions: &Sessions) -> Result<String, String> {
    if sessions.lock().unwrap().contains_key(name) {
        return Err(format!("session '{name}' already exists"));
    }
    let pair = native_pty_system().openpty(size(ROWS, COLS)).map_err(err)?;
    let mut cmd = CommandBuilder::new("/bin/sh");
    cmd.args(["-c", &format!("exec {command}")]);
    cmd.env("TERM", "xterm-256color");
    if let Ok(home) = env::var("HOME") {
        cmd.cwd(home);
    }
    let mut child = pair.slave.spawn_command(cmd).map_err(err)?;
    drop(pair.slave); // so the reader sees EOF when the child exits

    let mut reader = pair.master.try_clone_reader().map_err(err)?;
    let writer = pair.master.take_writer().map_err(err)?;
    let parser = Arc::new(Mutex::new(vt100::Parser::new(
        NonZeroU16::new(ROWS).unwrap(),
        NonZeroU16::new(COLS).unwrap(),
        1000,
    )));
    let feed = parser.clone();
    thread::spawn(move || {
        let mut buf = [0u8; 8192];
        while let Ok(n @ 1..) = reader.read(&mut buf) {
            feed.lock().unwrap().process(&buf[..n]);
        }
    });

    let pid = child.process_id();
    let mut map = sessions.lock().unwrap();
    if map.contains_key(name) {
        let _ = child.kill();
        return Err(format!("session '{name}' already exists"));
    }
    map.insert(
        name.to_string(),
        Arc::new(Session {
            pid,
            cmd: command.to_string(),
            child: Mutex::new(child),
            writer: Mutex::new(writer),
            master: Mutex::new(pair.master),
            parser,
        }),
    );
    Ok(format!("{}\n", pid.map_or("-".into(), |p| p.to_string())))
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
