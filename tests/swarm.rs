//! Scale and concurrency: many sessions, many clients, churn, and leak checks.

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn two_hundred_sessions_spawned_concurrently() {
    let d = Arc::new(Daemon::start());
    let handles: Vec<_> = (0..20)
        .map(|t| {
            let d = d.clone();
            thread::spawn(move || {
                for i in 0..10 {
                    let n = t * 10 + i;
                    d.ok(&format!("spawn w{n} sh -c 'echo ready-$((1000+{n})); sleep 120'"));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(d.ok("list").lines().count(), 200);
    for n in 0..200 {
        d.wait(&format!("w{n}"), &format!("ready-{}", 1000 + n));
    }
    let pids: Vec<u32> = d.ok("list").lines().map(|l| l.split('\t').nth(1).unwrap().parse().unwrap()).collect();
    // kill them all concurrently too
    let handles: Vec<_> = (0..20)
        .map(|t| {
            let d = d.clone();
            thread::spawn(move || {
                for i in 0..10 {
                    d.ok(&format!("kill w{}", t * 10 + i));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(d.ok("list"), "");
    wait_until("all 200 dead", Duration::from_secs(10), || pids.iter().all(|&p| !proc_alive(p)));
}

#[test]
fn mixed_command_storm_from_32_clients() {
    let d = Arc::new(Daemon::start());
    for i in 0..16 {
        d.bash(&format!("s{i}"));
    }
    let bad = Arc::new(AtomicUsize::new(0));
    let total = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..32)
        .map(|t| {
            let (d, bad, total) = (d.clone(), bad.clone(), total.clone());
            thread::spawn(move || {
                let mut rng = Rng(0x1234_5678 + t);
                for _ in 0..250 {
                    let s = format!("s{}", rng.below(16));
                    let req = match rng.below(12) {
                        0 => "list".to_string(),
                        1 => format!("info {s}"),
                        2 => format!("screen {s}"),
                        3 => format!("history {s} {}", rng.below(50)),
                        4 => format!("send {s} echo {}\\r", rng.next() % 100_000),
                        5 => format!("keys {s} echo {} Enter", rng.next() % 100_000),
                        6 => format!("resize {s} {} {}", 10 + rng.below(50), 20 + rng.below(150)),
                        7 => format!("wait {s} 50 $"),
                        8 => format!("screen nope{}", rng.below(5)),
                        9 => format!("keys {s} C-l"),
                        10 => format!("rename nope{} x", rng.below(5)),
                        _ => format!("send {s} \\e[A\\e[B"),
                    };
                    let r = d.req(&req);
                    total.fetch_add(1, Ordering::Relaxed);
                    if !(r.starts_with("OK\n") || r.starts_with("ERR ")) {
                        bad.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(total.load(Ordering::Relaxed), 8000);
    assert_eq!(bad.load(Ordering::Relaxed), 0);
    // Every shell still works afterwards. Random escapes can leave readline
    // mid-sequence (it would eat the next key), so reset it first and retry.
    for i in 0..16 {
        d.ok(&format!("resize s{i} 40 120"));
        let ok = (0..5).any(|attempt| {
            d.ok(&format!("send s{i} \\x03"));
            thread::sleep(Duration::from_millis(600)); // past readline's keyseq timeout
            d.ok(&format!("send s{i} echo $(({}+{i}))\\r", 7000 + attempt * 100));
            d.req(&format!("wait s{i} 3000 {}", 7000 + attempt * 100 + i)).starts_with("OK")
        });
        assert!(ok, "s{i} stopped responding:\n{}", d.screen(&format!("s{i}")));
    }
}

#[test]
fn spawn_kill_churn_leaks_nothing() {
    let d = Daemon::start();
    // warm up so lazily created runtime state is not counted as a leak
    d.ok("spawn warm true");
    d.ok("kill warm");
    thread::sleep(Duration::from_millis(200));
    let (fds, threads) = (fd_count(d.pid()), thread_count(d.pid()));
    for i in 0..300 {
        let name = format!("c{}", i % 7);
        d.ok(&format!("spawn {name} sh -c 'echo hi; sleep 30'"));
        if i % 3 == 0 {
            d.ok(&format!("send {name} x"));
            d.ok(&format!("screen {name}"));
        }
        d.ok(&format!("kill {name}"));
    }
    wait_until("fds and threads back to baseline", Duration::from_secs(10), || {
        fd_count(d.pid()) <= fds && thread_count(d.pid()) <= threads
    });
}

#[test]
fn exited_sessions_release_their_threads() {
    let d = Daemon::start();
    let threads = thread_count(d.pid());
    for i in 0..50 {
        d.ok(&format!("spawn e{i} true"));
    }
    // sessions stay listed (with their exit status) but their I/O threads end
    wait_until("io threads exit", Duration::from_secs(10), || thread_count(d.pid()) <= threads);
    assert_eq!(d.ok("list").matches("exited(0)").count(), 50);
}

#[test]
fn parallel_output_floods_across_sessions() {
    let d = Arc::new(Daemon::start());
    for i in 0..8 {
        d.ok(&format!("spawn f{i} sh -c 'seq 1 300000; echo FLOOD-$((100+{i})); sleep 60'"));
    }
    let start = Instant::now();
    let mut worst = Duration::ZERO;
    loop {
        let t = Instant::now();
        d.ok("list");
        worst = worst.max(t.elapsed());
        let done = (0..8).filter(|i| d.screen(&format!("f{i}")).contains(&format!("FLOOD-{}", 100 + i))).count();
        if done == 8 {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(60), "floods did not finish");
    }
    assert!(worst < Duration::from_millis(250), "list latency under 8 floods: {worst:?}");
    for i in 0..8 {
        let last: Vec<String> = d.ok(&format!("history f{i} 5")).lines().map(String::from).collect();
        assert!(last.iter().any(|l| l == "300000"), "f{i}: {last:?}");
    }
}

#[test]
fn twenty_attachers_and_a_writer() {
    let d = Daemon::start();
    d.bash("s");
    let clients: Vec<_> = (0..20)
        .map(|_| {
            std::process::Command::new(BIN)
                .args(["attach", "s"])
                .env("XUMUX_SOCK", &d.sock)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    wait_until("20 attached", Duration::from_secs(10), || d.info("s", "attached") == "20");
    d.ok("send s seq 1 20000; echo FANOUT-$((40+2))\\r");
    d.ok("wait s 20000 FANOUT-42");
    thread::sleep(Duration::from_millis(300));
    for mut c in clients {
        use std::io::Write;
        c.stdin.as_mut().unwrap().write_all(&[0x1d]).unwrap();
        let out = String::from_utf8_lossy(&c.wait_with_output().unwrap().stdout).into_owned();
        assert!(out.contains("FANOUT-42"), "an attached client missed output");
    }
    wait_until("all detached", Duration::from_secs(5), || d.info("s", "attached") == "0");
}

#[test]
fn rapid_resize_storm_on_a_tui() {
    let d = Daemon::start();
    d.ok("spawn top top -d 0.1");
    d.wait("top", "load average");
    let mut rng = Rng(42);
    for _ in 0..500 {
        d.ok(&format!("resize top {} {}", 1 + rng.below(200), 1 + rng.below(300)));
    }
    d.ok("resize top 40 120");
    d.wait("top", "load average");
    d.ok("keys top q");
}

#[test]
fn many_concurrent_waiters() {
    let d = Arc::new(Daemon::start());
    d.bash("s");
    let handles: Vec<_> = (0..50)
        .map(|_| {
            let d = d.clone();
            thread::spawn(move || d.ok("wait s 10000 GO-77"))
        })
        .collect();
    thread::sleep(Duration::from_millis(300));
    d.ok("send s echo GO-$((70+7))\\r");
    for h in handles {
        h.join().unwrap();
    }
}
