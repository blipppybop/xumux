# xumux: guide for AI agents

xumux keeps programs running in real terminals between your tool calls. Use it whenever you need something that **stays alive** (a shell whose `cd` and env persist, a dev server, a REPL, a long build) or something **interactive** (a TUI, a prompt, another agent's CLI). Every command is a one-shot CLI call: exit code 0 means success, and on failure it exits 1 with the error on stderr.

Print this guide anytime with `xumux guide`.

## 0. Make sure the daemon is running

```sh
xumux list >/dev/null 2>&1 || (xumux serve >/dev/null 2>&1 &)
```

## 1. The core loop: `spawn`, then `run`

```sh
xumux spawn sh bash                    # a persistent shell named "sh"
xumux settle sh 10000 1000             # let it finish starting (.bashrc can be slow)
xumux run sh 30000 'cd ~/project && ls'
```

Token tip: a short prompt keeps every `run` result small:
`xumux spawn sh "env PS1='$ ' bash --norc"`.

`run NAME MS TEXT` types `TEXT`, presses Enter, and waits until the command is **done**: the shell is back at its prompt (no foreground job running) and output has been quiet for 300 ms. Then it prints **only the new output** plus a status line:

```
$ cd ~/project && ls
Cargo.toml  README.md  src
$
[xumux] quiet
```

| Status line | Meaning | What to do |
|---|---|---|
| `[xumux] quiet` | done: no foreground job, output stopped | read the output |
| `[xumux] timeout: CMD still running` | `CMD` is still running after `MS` | `xumux settle NAME 600000` to keep waiting, or `xumux keys NAME C-c` |
| `[xumux] timeout` | still printing after `MS` | `xumux settle NAME 60000`, then `xumux history NAME 40` |
| `[xumux] exited(N)` | the program itself ended (code `N`) | read the output; `kill` it, or `spawn` again |

The output starts with the prompt and your command, as on a real terminal. State persists between calls: `cd`, exported variables and activated virtualenvs all stay.

## 2. Recipes

**A dev server or other long-running process**
```sh
xumux spawn api 'cd ~/api && exec npm run dev'
xumux wait api 60000 'ready on || Error'   # prints whichever text appeared
xumux log api /tmp/api.log                 # optional: full raw output to a file
xumux stats api                            # cpu / memory of the whole process tree
```

**A long job: wait for it to finish and get the exit code**
```sh
xumux spawn build 'cd ~/proj && exec make -j8'
xumux wait-exit build 600000               # prints exited(0), or exits 1 on timeout
xumux history build 30                     # the last 30 lines
```

**A REPL (python, node, psql, ...)**
```sh
xumux spawn py python3
xumux settle py                            # let it print its banner
xumux run py 10000 'import sys; sys.version'
```

**A full-screen TUI (top, htop, vim, an installer)**
```sh
xumux spawn top top
xumux settle top                           # wait until it stops redrawing
xumux screen top                           # the whole screen as text
xumux keys top q                           # press keys
```
`run` and `screen` return the full screen while a program is in full-screen mode.

**Another AI agent's CLI**
```sh
xumux spawn grok grok
xumux settle grok 30000 1000               # wait for its UI to finish loading
xumux send grok 'Summarize README.md\r'    # \r is Enter
xumux settle grok 300000 3000              # replies pause: use a longer idle (3 s)
xumux screen grok
```

**Interrupt, answer prompts, navigate**
```sh
xumux keys sh C-c                          # Ctrl-C
xumux keys sh y Enter                      # answer a y/n prompt
xumux keys sh Up Enter                     # rerun the last command
xumux keys tui Down Down Enter             # move through a menu
```

**Clean up what you started**
```sh
xumux kill api                             # kills the program and everything it spawned
```

## 3. Command reference

| Command | Output on success |
|---|---|
| `spawn NAME COMMAND...` | the pid. `COMMAND` runs through `sh -c`, cwd `$HOME`, size 40×120 |
| `run NAME MS TEXT` | new output, then `[xumux] quiet\|timeout\|exited(N)` |
| `settle NAME [MS] [IDLE_MS]` | same status as `run`, without typing anything (defaults: 10000 ms, idle 300 ms) |
| `wait NAME MS TEXT [\|\| TEXT...]` | the text that appeared. Error on timeout, or if the program exits first |
| `wait-exit NAME MS` | `exited(N)` or `killed(sig N)`. Error on timeout |
| `send NAME TEXT` | nothing. Escapes: `\r` Enter, `\n`, `\t`, `\e` Esc, `\xHH`, `\\` |
| `keys NAME KEY...` | nothing. `Enter Tab Esc BSpace Space Up Down Left Right Home End PgUp PgDn Delete F1`–`F12 C-a`…`C-z M-x`; any other word is typed as text |
| `screen NAME` | the visible screen, trailing blanks trimmed |
| `history NAME [LINES]` | scrollback + screen, or just the last `LINES` lines |
| `list` | one line per session: `name<TAB>pid<TAB>status<TAB>command` |
| `info NAME` | `key<TAB>value` lines: pid, status, foreground (the command running in the shell, or `-`), size, cursor, alt_screen, title, history, attached, log, command |
| `stats NAME` | `procs<TAB>N`, `cpu<TAB>X%`, `rss<TAB>Y MiB` (500 ms sample) |
| `log NAME PATH\|off` | nothing. Appends raw output (with colors) to a file |
| `resize NAME ROWS COLS` | nothing |
| `rename NAME NEW` / `kill NAME` | nothing |
| `shutdown` | `killed N sessions`; stops the daemon |

Status values: `running`, `exited(N)`, `killed(sig N)`. Names are 1–64 characters of `A-Z a-z 0-9 . _ -`.

## 4. Rules that save you retries

1. **Enter is `\r`.** `run` adds it for you; with `send` you must add it yourself (`'ls\r'`). `keys ... Enter` also works.
2. **Single-quote the whole text** so your own shell leaves `$`, `*` and `;` alone: `xumux run sh 5000 'echo $HOME; ls *.rs'`.
3. **`wait` also sees what you typed.** `xumux send sh 'make; echo DONE\r'` followed by `wait sh 60000 DONE` matches the typed command right away. Make the marker something only the output contains: `echo DONE-$((1+1))`, then wait for `DONE-2`. Or just use `run` or `wait-exit`.
4. **In a shell, `run` knows when a command is still running**, even a silent one: it waits for the prompt to come back. In a program that isn't a shell (a REPL, a TUI, an agent CLI), `quiet` only means output paused. For replies that pause, use `settle NAME MS 3000` or wait for a known text.
5. **Keep your context small.** Prefer `run` (new output only) and `history NAME 20` over full `screen` or `history` dumps.
6. **Never `attach` from a tool call.** It's interactive and waits for a keyboard. It's for humans.
7. **Check before you assume.** `xumux list` shows whether a session is still `running` or has `exited(N)`.
8. **Clean up.** Kill the sessions you created when you're done. Exited sessions stay readable until killed; only the 256 most recent are kept.

## 5. Errors you may see

| Error | Meaning |
|---|---|
| `... is xumux serve running?` | start the daemon (step 0) |
| `no session 'x'` | wrong name; run `xumux list` |
| `session 'x' already exists` | reuse it, or `kill` it first |
| `timed out after N ms ...` | the text never appeared; check `screen` |
| `session exited before ... appeared` | the program ended; read `history NAME 30` |
| `input queue full ...` | the program isn't reading input (busy or hung); try `keys NAME C-c` |
| `busy: too many clients` | more than 512 connections open at once; retry after a moment |
| `internal error ...` | a bug in xumux; the session survives, the daemon keeps running |
