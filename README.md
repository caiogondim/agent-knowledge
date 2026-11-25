# agent-knowledge

Share a folder with live two-way sync over
[habilis-network](https://github.com/agent-habilis/habilis-network).

```bash
agent-knowledge share <folder>          # prints a hash
agent-knowledge mount <hash> <target>   # keeps <target> in sync with <folder>
agent-knowledge plug                    # installs the skills into Claude Code, Codex, and Pi
agent-knowledge status                  # prints this session's memory bank folder
```

### Skills

| Skill | What it does |
|---|---|
| `/knowledge-share <folder>` | Runs `share` in the background and prints the hash. |
| `/knowledge-mount <hash> <target>` | Runs `mount` in the background. The target is the memory bank. |
| `/knowledge-write <what>` | Writes an [OKF](https://github.com/GoogleCloudPlatform/open-knowledge-format) concept to the session's memory bank. Errors if there is none. |
| `/knowledge-read [question]` | Answers from the session's memory bank. Errors if there is none. |

### One memory per session

Each agent session runs at most one memory: one `share` or one `mount`. A
second one in the same session fails with `this session already runs a ...
memory`. `status` prints the folder of the running one, and
`/knowledge-write` and `/knowledge-read` use it.

The session is `$CLAUDE_CODE_SESSION_ID` in Claude Code, `$CODEX_THREAD_ID` in
Codex, or `$AGENT_KNOWLEDGE_SESSION`. Without any of them (Pi, a plain
terminal), everything shares the session `default`. Each session's memory is
recorded in `~/.agent-knowledge/sessions/<session>.json` (or under
`$AGENT_KNOWLEDGE_HOME`); an entry whose process died is ignored.

The table uses the Claude Code form. In Codex, a skill command starts with `$`
(`$knowledge-share`). In Pi, it starts with `/skill:` (`/skill:knowledge-share`).

`plug` writes the 4 skills into each agent that it finds on this machine, and
replaces older copies:

| Agent | Skills folder | Override | Skill command |
|---|---|---|---|
| Claude Code | `~/.claude/skills` | `$CLAUDE_CONFIG_DIR/skills` | `/knowledge-share` |
| Codex | `~/.codex/skills` | `$CODEX_HOME/skills` | `$knowledge-share` |
| Pi | `~/.pi/agent/skills` | `$PI_CODING_AGENT_DIR/skills` | `/skill:knowledge-share` |

`plug` finds an agent by its config folder (for example `~/.pi/agent`). To
pick agents, use `--agent claude-code`, `--agent codex`, or `--agent pi`, one
or more times. Restart the agent to load the skills.

## Install

The skills call `agent-knowledge` from `PATH`, so install the binary before
you plug the skills:

```bash
# From a clone
cargo install --path .

# Without a clone
cargo install --git https://github.com/caiogondim/agent-knowledge

# Then plug the skills and restart Claude Code, Codex, or Pi
agent-knowledge plug
```

`cargo install` puts the binary in `~/.cargo/bin`. If the shell cannot find
`agent-knowledge`, run `source ~/.cargo/env` or open a new terminal.

## Development

### Requirements

- Rust 1.95.0. `rust-toolchain.toml` pins this version, so `rustup` installs it on the first build.
- Read access to the `agent-habilis/habilis-network` GitHub repo. Cargo fetches it as a git dependency.

### Build

```bash
cargo build
```

### Run

**Caution:** the sync goes both ways. An edit or a delete in `<path>` also changes `<folder>`.

1. In terminal 1, share a folder. The command prints the hash on stdout:

   ```bash
   cargo run -- share <folder>
   ```

2. In terminal 2, mount the hash. If `<path>` does not exist, the command creates it:

   ```bash
   cargo run -- mount <hash> <path>
   ```

3. Edit, create, or delete a file on one side. The change appears on the other side in a few seconds.

4. To stop a side, press Ctrl-C.

### Test

```bash
cargo test
cargo clippy
```

- `src/scan.rs` has the unit tests.
- `tests/memory.rs` checks the session id, the registry, and the one-memory rule.
- `tests/plug.rs` checks the embedded skills, the skills folder and the detection of each agent, and the install.
- `tests/two_way.rs` runs a `share` loop and a `mount` loop in 1 process. It checks a create, an edit, and a delete in both directions. It takes about 10s.

### Code map

- `src/main.rs`: the CLI, with the `share`, `mount`, and `plug` commands.
- `src/memory.rs`: the one-memory-per-session registry behind `share`, `mount`, and `status`.
- `src/plug.rs`: embeds `src/skills/*/SKILL.md` in the binary and writes them to an agent's skills folder.
- `src/skills/`: the skills that `plug` installs. Edit them here, then rebuild.
- `src/sync.rs`: the sync loop. Both commands run it.
- `src/view.rs`: the merge rule for the file list. It has no I/O.
- `src/scan.rs`: walks a folder and hashes each file. It also rejects unsafe paths from a peer.
- `src/watch.rs`: the file watcher. It sends 1 tick after 300ms with no changes.

### How it works

- **The hash** is a habilis-network mesh id. The mesh uses all lookups: mDNS, DHT, and relay.
- **The file list** is the mesh `state` document. Each peer writes its full view of the folder into its own signed slice. Each entry holds the sha256 of a file, a blob ticket, and a `clock`. A delete is an entry with `deleted: true`.
- **The file bytes** go through the blob ticket, because a gossip frame holds at most 3840 bytes. A download runs in the background and fails when no byte arrives for 10 s.
- **Echo stop:** each side keeps the last synced sha256 of each path. A change that matches it is the side's own echo, so the side skips it.
- **Conflicts:** for each path, the entry with the highest `clock` wins. On a tie, a file beats a delete, and between 2 files the larger sha256 wins. A local change gets a clock 1 above the highest clock in all slices.
- **Adoption:** a peer that applies a winner copies it into its own slice, with its own blob ticket. So the folder survives when the share stops, and a fetch has more than 1 source.
- **Restart:** each peer saves the last synced state of each path, for each mesh and folder, in `$AGENT_KNOWLEDGE_HOME/known/` (default `~/.agent-knowledge/known/`). A file that differs from it at start is an offline edit, and it wins. A saved delete is published again too.
- **Join:** a file in the folder at join that no earlier run synced yields to the mesh. If a peer already holds that path as a file, its version overwrites the file. A path that no peer holds, or that peers deleted, is published.

### Limits

- Hidden files and folders (`.git`, `.DS_Store`) do not sync.
- Empty folders do not sync.
- A rename syncs as a delete plus a create.
- Each side keeps a full copy of the folder.
- One slice holds at most 1 MiB. An entry takes about 520 bytes, so a folder holds about 2000 files, deleted files included. Delete entries are never removed. Above the limit, a peer logs an error and stops publishing. It still downloads.
- Peers that run an older build do not meet peers that run this one: the mesh ids differ.
