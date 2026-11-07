<div align="center">

# xumux

### The zero-dependency terminal multiplexer for programs, scripts and AI agents

`0 dependencies` · `1 Rust file` · `635 KiB binary` · `119 tests` · `Linux` · `MIT`

</div>

---

> ## Abstract
>
> **xumux runs programs inside real terminals and lets anything on your machine drive them**: start a program, type into it, read its screen, wait for output, watch its CPU and memory, attach to it interactively, and kill its whole process tree.
>
> **Zero dependency.** It's built from the Rust standard library and nothing else. `Cargo.lock` lists exactly one package: xumux itself. There's no third-party crate, no C library beyond the libc that std already links, no build script and no code generation. The terminal emulator, PTY handling, protocol and process monitoring are all written here, in one file you can read in an afternoon.
>
> **Fully local.** There's no network code at all. xumux talks over a Unix socket that only your user can open, in a directory that must be yours alone (mode `700`, not a symlink) or it refuses to run. It never phones home, never downloads anything, and builds offline.
>
> **Open source.** MIT licensed. Everything that runs is in this repository: no binary blobs, no vendored code, no supply chain to trust.

---

## At a glance

|                         | **xumux**                                     | tmux                                     |
|-------------------------|-----------------------------------------------|------------------------------------------|
| Source                  | **2,185 lines**, 1 file                       | 114,498 lines, 157 files                 |
| Dependencies            | **none**                                      | libevent, ncurses (+ utf8proc, jemalloc) |
| Binary                  | **635 KiB**, statically linked Rust           | plus shared libraries                    |
| Clean build             | **4.8 s**, offline                            | autotools + library headers              |
| Idle daemon memory      | **2.5 MiB**                                   |                                          |
| Built for               | programs and agents (people via `attach`)     | people at a keyboard                     |
| Memory safety           | Rust; `#![deny(unsafe_code)]`, 9 libc calls   | C                                        |

<sub>Total lines of source, including comments and blanks; xumux's count leaves out its tests. tmux counted at commit `a73616d` (2026-10-30).</sub>

## Quick start

```sh
git clone https://github.com/blipppybop/xumux.git && cd xumux
cargo build --release                  # downloads nothing
install -m 755 target/release/xumux ~/.local/bin/

xumux serve &                          # the daemon
xumux spawn sh bash                    # a shell that persists between calls
xumux run sh 30000 'cd ~/api && ls'    # type, wait until done, print only the new output
xumux spawn api 'cd ~/api && PORT=8080 exec ./server'
xumux wait api 10000 'listening || Error'   # block until either text shows up
xumux stats api                        # CPU and memory of its process tree
xumux attach api                       # take the keyboard; Ctrl-] detaches
```

**Using it from an AI agent?** Read [`AGENTS.md`](AGENTS.md), or run `xumux guide`: the same text is built into the binary.

## Commands

| xumux | Does | tmux equivalent |
|---|---|---|
| `serve` | run the daemon | `start-server` |
| `spawn NAME COMMAND...` | start a program in a new 40×120 PTY | `new-session -d` |
| `list` | name, pid, status, command | `list-sessions` |
| `info NAME` | pid, status, size, cursor, title, alt screen, history, viewers, log | `display -p` |
| `send NAME TEXT` | type text; escapes `\r \n \t \e \\ \xHH` | `send-keys -l` |
| `keys NAME KEY...` | `Enter Tab Esc BSpace Up Down Left Right Home End PgUp PgDn F1`–`F12 C-x M-x`, or plain text | `send-keys` |
| `screen NAME` | the visible screen as plain text | `capture-pane -p` |
| `history NAME [LINES]` | scrollback + screen (keeps 10,000 lines) | `capture-pane -p -S -` |
| `run NAME MS TEXT` | type `TEXT` + Enter, wait until the command is done, print only the new output | *none* |
| `settle NAME [MS] [IDLE]` | wait until output stops and no foreground job is running | *none* |
| `wait NAME MS TEXT [\|\| TEXT]` | block until any of the texts is on screen; print which | *none (scripts poll)* |
| `wait-exit NAME MS` | wait for the program to exit; print `exited(N)` | `wait-for` |
| `guide` | print the agent guide | *none* |
| `log NAME PATH\|off` | append raw output to a file | `pipe-pane` |
| `stats NAME` | CPU % and RSS of the whole process tree | *none* |
| `attach NAME` | interactive; `Ctrl-]` detaches; any number of viewers | `attach` |
| `resize NAME ROWS COLS` | resize the PTY; the program gets `SIGWINCH` | `resize-window` |
| `rename NAME NEW` | rename a session | `rename-session` |
| `kill NAME` | kill the program, its children and its orphans | `kill-session` |
| `shutdown` | kill every session and stop the daemon | `kill-server` |

The working directory and environment come from the command itself: `xumux spawn job 'cd /srv && DEBUG=1 exec ./run'`.

**Deliberately left out:** split panes, window layouts, the status bar, copy mode, key bindings and the config language. Those are tmux's user interface, and your terminal app or window manager already does them. Open `xumux attach` in as many tabs or splits as you like.

## Benchmarks

Intel Core i5-14500 (20 threads), Linux 6.14, Rust 1.97, release build. Latencies are measured over the raw socket unless noted, 10,000 samples after warm-up.

| Benchmark | Result |
|---|---|
| Daemon start → accepting connections | **2.8 ms** |
| Idle daemon memory (RSS) | **2.7 MiB** |
| `list` round trip | **214 µs** median · 353 µs p99 |
| `send` round trip | **210 µs** median · 327 µs p99 |
| `info` round trip | **280 µs** median · 412 µs p99 |
| `screen` round trip (full 40×120 screen) | **301 µs** median · 443 µs p99 |
| `xumux list` from a shell (process start + request) | **3.4 ms** median |
| Throughput, 16 concurrent clients | **13,000 requests/s** |
| `spawn` (PTY + `posix_spawn`), with 200 sessions open | **1.9 ms** median · 24.8 ms p99 |
| Spawn → first output seen by `wait` | **13 ms** median |
| Daemon with 250 sessions open | **25.7 MiB** RSS · 2 threads per running session |
| `kill` (whole process tree, reaped) | **17 ms** median |
| Output throughput, PTY → emulator (200 MiB) | **75 MiB/s** |

Reproduce with:

```sh
cargo test --release --test bench -- --ignored --nocapture --test-threads=1
```

Two design choices behind these numbers:

- **`spawn` never forks the daemon.** Programs start as `/proc/self/exe __pty-exec CMD` via `posix_spawn`, and that helper takes the terminal and then execs the shell. `fork()` copies the parent's memory map, and the daemon's grows with every session: `spawn` used to take 12 ms with 200 sessions open, and now takes 1.6 ms at any count.
- **Nothing waits on anything slow.** Each session has a writer thread with a bounded input queue, and each attached viewer has its own sender with a bounded output queue. A program that doesn't read its input, or a viewer that doesn't read its output, can't stall the session or the daemon.

## Robustness

A multiplexer is only useful if it outlives the programs inside it. Design rules, each with a test:

- **A bug never takes the daemon down.** Panics unwind instead of aborting. A panic while handling a request is caught and answered with `ERR internal error`; a panic inside the terminal emulator resets that one session's screen and keeps the session; a poisoned lock is recovered instead of cascading. The emulator is also fuzzed with ~900,000 random inputs so this path should never be needed.
- **Everything is bounded.** Request size (1 MiB), request time (5 s), pending input per session (4 MiB), viewer lag (8 MiB), concurrent clients (512, then `ERR busy`), exited sessions kept for reading (256, oldest pruned), PTY size (1000×1000), scrollback (10,000 lines), name length (64).
- **Clients never panic on a closed pipe.** `xumux history s | head -1` exits quietly; a daemon started with stderr closed still runs.
- **Attach always lets go.** A viewer that disconnects is unregistered right away, even from a silent session, and `Ctrl-]` gets through within 2 s even when pasting into a program that has stopped reading its input.
- **`run` knows the difference between "quiet" and "done".** It waits for the shell to have no foreground job *and* not be runnable, so a command that is still being forked under heavy load isn't reported finished.

## Security

The threat model has two parts: other users on the same machine, and hostile programs running inside sessions. Anyone who can connect to your socket can run commands as you; that's what the tool is for, and it's the same trust model as tmux. xumux makes sure nobody else can connect, and that programs can't escape through the terminal.

**Guarantees, each tested in [`tests/security.rs`](tests/security.rs):**

- **Private socket.** The socket is mode `0600`. Its directory, including one you pick with `$XUMUX_SOCK`, must be a real directory (not a symlink) that you own with mode `700`, or xumux refuses to run. That's what keeps other users out, and stops anyone from pre-creating or swapping the socket. Without `$XDG_RUNTIME_DIR` it uses `/tmp/xumux-$UID`, created with mode `0700`.
- **Bounded protocol.** Requests are capped at 1 MiB, must be UTF-8, and must arrive within 5 s in total, so sending one byte at a time doesn't help. Idle connections can't pin threads, and error replies always reach the client.
- **No fd leaks.** Every descriptor is close-on-exec. A program sees exactly stdin, stdout and stderr on its own PTY: never another session's terminal, the socket, or a log file.
- **Real isolation per session.** Each program leads its own session with its own controlling terminal, and every descriptor above stdio is closed before it starts. `kill` stops the leader, then rescans `/proc` until no live member remains: descendants, reparented orphans that kept the session id, and children that ignore SIGHUP.
- **Terminal output can't attack the reader.** `screen`, `history` and window titles never contain control characters. C1 controls (`U+009B` is CSI on some terminals) and bidi overrides ("Trojan Source") are dropped, so printing `xumux screen` is safe.
- **Hostile programs can't stall xumux.** Query floods, output floods, raw `/dev/urandom`, unread input and stalled viewers are all tested. Pending input is capped at 4 MiB per session.
- **Safe logging.** `log` opens only regular files, never through a symlink, non-blocking, with mode `0600`. A FIFO with no reader fails right away instead of hanging the session.

**Found and fixed during the audits:** 36 issues, 18 of them security-relevant. The last seven came from an independent audit by a second model whose probes now live in [`tests/audit_probes.rs`](tests/audit_probes.rs): descriptors inherited from the daemon's parent reached sessions; `kill` missed SIGHUP-ignoring children forked during its single `/proc` scan (it now stops the leader and rescans); a program's self-chosen `comm` could carry escape codes into `info`/`settle`; `log` followed symlinks; rejected connections past the client cap each spawned an uncounted thread; U+061C reached `screen`; and `poll`'s `nfds` was declared `u64`, wrong on 32-bit targets. They include a reply-flood deadlock in raw mode, a writer thread leaked when a program stopped reading, slow-loris requests, a `/tmp` socket hijack, a group-writable socket window under `umask 002`, unbounded queued input, escape injection through `screen`, stalled viewers slowing a session, and `log` hanging on a FIFO. Each one now has a regression test.

## Zero dependencies, in detail

The crate is built with `#![deny(unsafe_code)]`. The one exemption is the `sys` module, where 13 single-call `unsafe` blocks wrap the 9 libc functions std has no safe API for: `ioctl` (PTY unlock, window size, controlling terminal), `setsid`, `kill`, `poll`, `close`/`close_range` (so a session never inherits a descriptor from the daemon's parent), and `tcgetattr`/`tcsetattr`/`cfmakeraw` for `attach`'s raw mode. Clippy enforces one call per block and a `SAFETY` comment on each. Anything that *can* be done without a syscall is: the uid is read from `/proc/self/status`, the PTY number from `/proc/self/fdinfo`, and non-blocking mode is set through `OpenOptions`. `kill` refuses pid 0, pid 1 and anything that would become negative, because those values target whole process groups. Everything else is safe Rust:

- **PTYs** are opened through `/dev/ptmx` with std's `OpenOptions`, and processes are managed with std's `Command`/`Child`.
- **The terminal emulator** handles cursor movement, erase, insert/delete, scroll regions, the alternate screen, UTF-8 and double-width characters, window titles, 10,000 lines of scrollback, and answers cursor-position and device-attribute queries. It has unit tests and a randomized stress test, because a panic would take the daemon down.

The cost is that this code is ours to maintain instead of borrowed. That's the trade: there's nothing to audit but this repository.

## Tests

```sh
cargo test --release          # 119 tests: unit, security, commands, swarm, audit probes
cargo test                    # the same in debug, with overflow checks
```

| Suite | Tests | Covers |
|---|---|---|
| unit (`src/main.rs`) | 17 | emulator behavior, sanitizing, name rules, lock recovery, ~900,000-input stress test |
| [`security.rs`](tests/security.rs) | 34 | socket and directory checks, protocol abuse, isolation, hostile programs |
| [`commands.rs`](tests/commands.rs) | 52 | every command and its errors, through the socket and the real CLI; `run`/`settle`/`wait-exit` on shells, a REPL and a TUI; closed pipes, pruning, edge cases |
| [`swarm.rs`](tests/swarm.rs) | 8 | 200 concurrent sessions, 32 clients × 250 mixed commands, 300 spawn/kill cycles with leak checks, parallel floods, 20 viewers, resize storms |
| [`audit_probes.rs`](tests/audit_probes.rs) | 8 | reproductions from an independent audit: inherited fds, fork-loop orphans, comm injection, symlinked logs, busy-thread growth, bidi marks, oversize requests, fd release |

Each test runs its own daemon on its own private socket.

## Protocol

One line in; `OK\n<body>` or `ERR <message>\n` out; then the connection closes. Any language with Unix sockets can be a client:

```sh
printf 'list\n' | nc -U "$XDG_RUNTIME_DIR/xumux.sock"
```

`attach` is the one exception. After `OK\n` the connection carries a redraw and then raw output, and anything you write back is keystrokes. Byte `0x1d` (`Ctrl-]`) detaches.

## Limitations

- **Linux only:** x86, x86_64, arm, aarch64 and riscv64. Other targets fail at compile time rather than misbehave.
- **Sessions live in the daemon.** Restarting `xumux serve` ends them, just like `tmux kill-server`. Exited sessions stay listed (output readable) until you `kill` them or 256 newer ones have exited.
- **Programs that daemonize** (double-fork into a new session) escape `kill`, as they do in tmux.
- **`screen` and `history` are text only.** Colors and combining accents are dropped; `log` and `attach` keep the raw bytes, colors included.
- **Character widths** come from a built-in table that covers CJK and common emoji. Rare symbols may be measured differently than the program expects.

## License

[MIT](LICENSE)
