# Functionality dropped (or planned to be dropped) at rebase onto upstream v0.3.0+

This file lists fork features that have been (or will be) dropped during the rebase onto upstream. Each entry names the squashed commit on the **pre-rebase** branch where the code last existed, so a future session can `git show <hash>` or cherry-pick if any of it needs to come back.

Backup branch: **`pre-squash`** at `ae5f403a551849dad04352ca1db926306718581c` — preserves the original 40-commit fork history before the 2026-05-21 squash.

Squashed-branch reference (these are the hashes inside the 8-commit fork on top of upstream `ef7ab49`, **before** rebasing onto a newer upstream):

| Squashed hash | Subject |
|---|---|
| `ce73bf0` | feat: guaranteed jobs — retry, startup replay, multi-client leader election |
| `43f104f` | feat: persist widget visibility across sessions |
| `1f89bee` | feat: scheduled-prompt UX polish |
| `eb1ffc2` | infra: repoint deps to local pi-mono; rename schema params |
| `134aab4` | fix: agent-turn gating, stuck-state watchdog, silent race deferral |
| `2731360` | feat: context-aware routing for RPC bridges |
| `d3db3a0` | feat: dedicated contexts for standalone tasks |
| `2c40a69` | feat: command mode for shell-only scheduled prompts |

These hashes will change after rebasing — re-record them post-rebase.

---

## Planned drops at next rebase

### Dedicated contexts for standalone tasks
**Lives in:** `d3db3a0`
**Why drop:** Upstream v0.3.0 added a `model:` field that runs scheduled prompts as in-process subagents (`src/subagent.ts`), covering the same need with less code. The fork's version is more isolated (separate `pi` subprocess) and adds features upstream lacks — see below.

**What's lost:**
- Process-level isolation (true `--mode json -p` subprocess per job vs in-process AgentSession)
- Run history / replay UI: `/schedule-prompt replay <jobId>` to inspect past dedicated runs
- `/schedule-prompt ps` to list currently-running dedicated subprocesses
- Dedicated session log streaming back into the main session
- Subprocess preservation across session replacement (e.g. `/new`)
- 20-minute retry watchdog for stuck dedicated jobs
- "Exit code 0 but empty stdout" failure detection
- The `resyncTimersFromStorage` reconciliation (a dedicated child that calls `schedule_prompt` updates only the disk file — the host scheduler needs to pick up the change)
- `--no-extensions` toggle on the spawned `pi` (was being revisited in HEAD anyway)
- `context/DEDICATED_CONTEXT_PRESERVATION.md` (delete on rebase — it documents the dropped behavior)

**To revisit on top of upstream:** if any of replay/ps/log-streaming is wanted later, port it onto upstream's `runSubagentOnce()` path instead of replaying this commit.

### Persisted widget visibility
**Lives in:** `43f104f`
**Why drop:** Upstream v0.3.0 added `src/settings.ts` which persists `widgetVisible` (plus `defaultJobScope`) in a separate `.pi/schedule-prompts-settings.json`. More general than the fork's "stuff it into the main storage file" approach.

**What's lost:** Nothing functional — same flag, different file. Users with a persisted `widgetVisible=false` in the legacy location will see the widget reappear once after rebase; they can hide it again and it'll persist via upstream's settings module.

### Schema renames (`type` → `jobType`, `description` → `jobDescription`)
**Lives in:** `eb1ffc2`
**Why drop:** Upstream uses the standalone `typebox` package (same as the fork) and keeps `type` and `description` as parameter names — they work fine. The original justification ("TypeBox reserved keys") no longer applies. Reverting drops dozens of would-be conflicts against upstream's new tool/UI code.

**What's lost:** Any external agent prompt or tool caller that learned the renamed names will need to switch back. The MCP-style tool schema stays compatible with upstream after this drop.

**Note:** `eb1ffc2` also contains real infra changes (local `file:` pins, package-lock rewrite, vitest setup, typebox import switch). Keep those — the drop is *only* of the type/description renames. Likely needs a hand-split during rebase, not a wholesale `git rebase --skip`.

---

## Kept (do not drop)

For reference — these were considered as drop candidates but are being kept:

- `targetContext` field on `CronJob` (`2731360`) — different purpose from upstream's `session` field (RPC routing vs session binding); both coexist.
- `command:` mode (`2c40a69`) — no upstream equivalent; orthogonal to upstream's `model:` subagent path.
- All guaranteed-job machinery (`ce73bf0`, `134aab4`) — upstream has nothing comparable; this is core to fork's value.
- UX polish (`1f89bee`) — small, low-risk, and upstream's new UI may benefit from these utilities.
