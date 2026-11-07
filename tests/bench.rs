//! Benchmarks. Not run by default:
//!
//!     cargo test --release --test bench -- --ignored --nocapture --test-threads=1
//!
//! Prints Markdown table rows.

mod common;
use common::*;

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

fn pct(mut v: Vec<Duration>) -> (Duration, Duration) {
    v.sort();
    (v[v.len() / 2], v[v.len() * 99 / 100])
}

fn us(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us >= 1000.0 {
        format!("{:.2} ms", us / 1000.0)
    } else {
        format!("{us:.0} µs")
    }
}

fn rss_kib(pid: u32) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap()
}

fn row(what: &str, value: String) {
    println!("| {what} | {value} |");
}

fn latency(d: &Daemon, req: &str, n: usize) -> (Duration, Duration) {
    for _ in 0..200 {
        d.req(req);
    }
    pct((0..n)
        .map(|_| {
            let t = Instant::now();
            let r = d.req(req);
            let e = t.elapsed();
            assert!(r.starts_with("OK"), "{req}: {r}");
            e
        })
        .collect())
}

#[test]
#[ignore]
fn bench() {
    println!("\n| Benchmark | Result |\n|---|---|");

    // ---- startup
    let starts: Vec<Duration> = (0..20)
        .map(|_| {
            let dir = private_dir();
            let sock = dir.join("b.sock");
            let t = Instant::now();
            let mut c = Command::new(BIN)
                .arg("serve")
                .env("XUMUX_SOCK", &sock)
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            while UnixStream::connect(&sock).is_err() {
                thread::sleep(Duration::from_micros(50));
            }
            let e = t.elapsed();
            c.kill().unwrap();
            c.wait().unwrap();
            std::fs::remove_dir_all(dir).unwrap();
            e
        })
        .collect();
    row(
        "Daemon start → accepting connections (median)",
        us(pct(starts).0),
    );

    let d = Daemon::start();
    thread::sleep(Duration::from_millis(100));
    row(
        "Daemon memory, idle (RSS)",
        format!("{:.1} MiB", rss_kib(d.pid()) as f64 / 1024.0),
    );

    // ---- request latency (raw socket, no process spawn)
    d.ok("spawn sh env PS1='$ ' bash --norc --noprofile -i");
    d.wait("sh", "$");
    d.ok("send sh for i in $(seq 1 40); do printf '%0119d\\n' $i; done\\r");
    thread::sleep(Duration::from_millis(300));
    for (label, req) in [
        ("`list` round trip", "list"),
        ("`info` round trip", "info sh"),
        ("`screen` round trip (full 40×120 screen)", "screen sh"),
        ("`send` round trip", "send sh \\x00"),
    ] {
        let (p50, p99) = latency(&d, req, 10_000);
        row(label, format!("{} median · {} p99", us(p50), us(p99)));
    }

    // ---- CLI end to end (includes exec of the client binary)
    let cli: Vec<Duration> = (0..300)
        .map(|_| {
            let t = Instant::now();
            assert!(d.cli(&["list"]).status.success());
            t.elapsed()
        })
        .collect();
    row(
        "`xumux list` from a shell (process start + request)",
        format!("{} median", us(pct(cli).0)),
    );

    // ---- throughput: 16 concurrent clients
    let stop = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicUsize::new(0));
    let sock = d.sock.clone();
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let (stop, count, sock) = (stop.clone(), count.clone(), sock.clone());
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let mut s = UnixStream::connect(&sock).unwrap();
                    s.write_all(b"info sh\n").unwrap();
                    let mut r = Vec::new();
                    s.read_to_end(&mut r).unwrap();
                    count.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    thread::sleep(Duration::from_secs(3));
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        w.join().unwrap();
    }
    row(
        "Throughput, 16 concurrent clients (`info`)",
        format!(
            "{:.0} requests/s",
            count.load(Ordering::Relaxed) as f64 / 3.0
        ),
    );

    // ---- spawn
    let spawns: Vec<Duration> = (0..200)
        .map(|i| {
            let t = Instant::now();
            d.ok(&format!("spawn p{i} sleep 600"));
            t.elapsed()
        })
        .collect();
    let (p50, p99) = pct(spawns);
    row(
        "`spawn` (PTY + posix_spawn), 200 sessions",
        format!("{} median · {} p99", us(p50), us(p99)),
    );
    let first_output: Vec<Duration> = (0..50)
        .map(|i| {
            let t = Instant::now();
            d.ok(&format!("spawn hi{i} echo hello-{i}"));
            d.ok(&format!("wait hi{i} 5000 hello-{i}"));
            t.elapsed()
        })
        .collect();
    row(
        "Spawn → first output visible to `wait`",
        format!("{} median", us(pct(first_output).0)),
    );
    thread::sleep(Duration::from_millis(300));
    let rss = rss_kib(d.pid());
    let threads = thread_count(d.pid());
    row(
        "Daemon with 250 sessions open",
        format!("{:.1} MiB RSS · {threads} threads", rss as f64 / 1024.0),
    );
    let kills: Vec<Duration> = (0..200)
        .map(|i| {
            let t = Instant::now();
            d.ok(&format!("kill p{i}"));
            t.elapsed()
        })
        .collect();
    row(
        "`kill` (whole process tree, reaped)",
        format!("{} median", us(pct(kills).0)),
    );

    // ---- emulator throughput
    let mb = 200usize;
    d.ok(&format!(
        "spawn flood sh -c 'head -c {} /dev/zero | tr \"\\0\" \"x\" | fold -w 100; echo FLOOD-END; sleep 60'",
        mb << 20
    ));
    let t = Instant::now();
    d.ok("wait flood 120000 FLOOD-END");
    let secs = t.elapsed().as_secs_f64();
    row(
        "Output throughput through PTY + emulator (200 MiB, 100-col lines)",
        format!("{:.0} MiB/s", mb as f64 / secs),
    );

    // ---- binary
    let size = std::fs::metadata(BIN).unwrap().len();
    row(
        "Binary size (release, stripped)",
        format!("{:.0} KiB", size as f64 / 1024.0),
    );
}
