# xumux

**A lean, local terminal multiplexer built for automation.**

xumux runs programs inside pseudo-terminals and lets anything on the machine — scripts, AI agents, other terminals — drive them through a tiny Unix-socket protocol: start a program, type into it, read its screen, check its CPU and memory, kill it.

It is the part of tmux you need for programmatic control, without the rest.

|                     | xumux                         | tmux                              |
|---------------------|-------------------------------|-----------------------------------|
| Source              | ~380 lines of Rust, one file  | ~70k lines of C                   |
| Binary              | ~650 KB, statically linked Rust deps | larger, links libevent + ncurses |
| Config              | none                          | config language, key tables       |
| Memory safety       | safe Rust (no `unsafe` in xumux) | C                              |
| Process stats       | built in (`stats`)            | not built in                      |
| Interactive UI      | no, headless only             | yes: panes, status bar, copy mode |

## Why

Tools like tmux are great for humans sitting at a keyboard. When the "user" is a program, such as an AI coding agent coordinating with another agent or a script babysitting a long-running TUI, most of that machinery is dead weight. xumux keeps only what automation needs:

- **Real PTYs.** Programs think they're in a normal terminal, so full-screen TUIs (`top`, editors, agent CLIs) work.
- **A parsed screen.** Output is fed through a VT100/xterm emulator ([`vt100`](https://crates.io/crates/vt100)), so `screen` returns what a human would *see*, not a stream of escape codes.
- **CPU and memory monitoring.** `stats` samples `/proc` for the session's whole process tree (the program plus every child it spawned).
- **Small enough to audit.** One file you can read in an afternoon. No config, no plugins, no network listener.

## Install

Needs a Rust toolchain (1.85+, edition 2024).

```sh
git clone https://github.com/blipppybop/xumux.git
cd xumux
cargo build --release
install -m 755 target/release/xumux ~/.local/bin/
```

## Usage

Start the daemon (foreground; run it under `systemd --user`, `nohup`, or a spare terminal):

```sh
xumux serve
```

Then from anywhere, as the same user:

```sh
xumux spawn sh bash -i              # start bash in a new PTY, prints its pid
xumux send sh 'echo hello\r'        # type into it (\r = Enter)
xumux screen sh                     # print what's on screen
xumux stats sh                      # CPU % and RSS of the whole process tree
xumux list                          # name, pid, status, command
xumux resize sh 50 160              # rows cols
xumux kill sh
```

Example `stats` output:

```
procs	3
cpu	100.0%
rss	8.6 MiB
```

### Commands

| Command                    | Effect                                                         |
|----------------------------|----------------------------------------------------------------|
| `spawn NAME COMMAND...`    | Run `COMMAND` via `sh -c 'exec …'` in a new 40×120 PTY, cwd `$HOME` |
| `send NAME TEXT`           | Write `TEXT` to the PTY. Escapes: `\r \n \t \e \\ \xHH`        |
| `screen NAME`              | Current visible screen as plain text                           |
| `list`                     | All sessions, tab-separated                                    |
| `stats NAME`               | Process count, CPU % (500 ms sample), total RSS                |
| `resize NAME ROWS COLS`    | Resize the PTY and the emulator                                |
| `kill NAME`                | Kill the process and drop the session                          |

Tips:
- Most TUIs submit on carriage return, so use `\r` rather than `\n` for Enter.
- `\x03` is Ctrl-C, `\e` is Escape, `\e[A` is the Up arrow.
- Quote `COMMAND` if it has its own quoting: `xumux spawn job 'python -c "print(1)"'`.

### Protocol

If you'd rather not shell out, talk to the socket directly. Send one line, read until EOF:

```
→ send sh echo hi\r
← OK
```
```
→ screen nope
← ERR no session 'nope'
```

Success is `OK\n` followed by the body; failure is `ERR <message>\n`. Any language with Unix sockets can be a client:

```sh
printf 'list\n' | nc -U "$XDG_RUNTIME_DIR/xumux.sock"
```

## Security model

- The socket lives at `$XUMUX_SOCK`, or else `$XDG_RUNTIME_DIR/xumux.sock`, and is created with mode `0600`. `$XDG_RUNTIME_DIR` is itself `0700` and per-user on systemd systems.
- **Anyone who can connect can run commands as you.** That's the point, and it's the same trust model as tmux's socket. Don't put the socket somewhere other users can reach.
- No network listener, no config file parsing, no `unsafe` code in xumux itself.
- Requests are capped at 1 MiB.

## Limitations

- Linux only for now (`stats` reads `/proc`; the PTY layer itself is portable).
- Sessions live in the daemon's memory; restarting `xumux serve` kills them.
- No attach mode: you can't take over a session interactively the way you can with `tmux attach`. It's built for programs, not people.
- The emulator doesn't answer terminal queries (such as a cursor-position request), so a few programs that block waiting for a reply may stall.
- Scrollback is kept (1000 lines) but isn't exposed yet; `screen` shows the visible area.

## Roadmap ideas

- `scrollback NAME` and `wait NAME PATTERN` (block until text appears)
- Per-session output logging to a file
- Optional JSON replies for structured clients
- Running `stats` continuously for whole-machine monitoring

## License

[MIT](LICENSE)
