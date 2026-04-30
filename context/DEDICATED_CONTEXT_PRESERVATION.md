# Preserving dedicated-context jobs across session replacement

## The problem

A `dedicatedContext: true` scheduled prompt runs in a freshly spawned `pi --mode json` subprocess so it stays out of the user's main session. The whole point is isolation: the main session shouldn't be able to see, influence, or be influenced by what the dedicated job is doing.

In practice, that isolation is broken in one direction. **When the user replaces the active session — `/new`, `/fork`, `/switch`, or `/reload` — every in-flight dedicated subprocess is killed.** The dedicated job has no semantic relationship to the user's chat history; it shouldn't care that the user typed `/new`. But it dies anyway, mid-run, sometimes minutes into a long task.

## Why it happens today

The cascade, end-to-end:

1. User invokes `/new`. pi-mono's `agent-session-runtime.newSession()` runs.
2. `teardownCurrent("new", …)` emits `session_shutdown` to all extensions.
3. Our handler in `src/index.ts` runs `cleanupSession(ctx)` → `scheduler.stop()`.
4. `stop()` walks `dedicatedJobControllers` and calls `controller.abort()` on each (`src/scheduler.ts:158-161`).
5. The abort signal was passed into `this.pi.exec("pi", […], { signal: controller.signal, … })` (`src/scheduler.ts:839`). pi-mono's exec implementation honors it and kills the child PID.
6. After teardown, pi-mono builds a fresh `AgentSession` and emits `session_start`. Our handler creates a brand-new `CronScheduler` from storage.
7. The new scheduler has no record that a subprocess was running. It re-evaluates next-run times from storage and resumes normal scheduling.

Net effect: a dedicated job that was 90% through its work is dead, its run record is never written, and from the new session's perspective the job simply "didn't happen this tick" (or was marked `error` if `guaranteed`).

## The trailing-notify bug we already fixed

Independent of subprocess preservation, there is a smaller race that the current patch addresses:

- `executeDedicatedJob` does `await this.pi.exec(...)` and then calls `notify("scheduled_prompt_end", …)` synchronously after the await resolves.
- If `/new` fires while the subprocess is still alive, `controller.abort()` runs — but pi-mono's exec resolves the promise (with `{ killed: true }`) rather than throwing `AbortError`.
- The continuation runs in the new tick. By that point, pi-mono has already disposed the old `AgentSession` and called `_extensionRunner.invalidate(...)`, so the captured `pi` is stale.
- The trailing `pi.sendMessage(...)` throws `"This extension ctx is stale after session replacement…"` and surfaces as an uncaught exception out of the cron tick.

The patch in `scheduler.ts` adds a `stopped` flag set in `stop()` and checked in `notify()`, plus a defensive `try`/`catch` that swallows stale-ctx errors. That keeps the new session quiet, but it does not bring the killed subprocess back.

## What "preserving dedicated jobs across `/new`" needs

Three problems to solve, roughly independent.

### 1. Don't abort the subprocess on session replacement

The minimal change. `session_shutdown` carries a `reason` field — pi-mono passes `"new"`, `"fork"`, `"switch"`, `"reload"`, or `"shutdown"`. Our extension should distinguish:

- `reason === "shutdown"` (process is exiting): abort everything, fine.
- Any other reason (session is being replaced but the host process keeps running): leave the subprocess alone.

That requires the `session_shutdown` handler to thread the reason into `cleanupSession`, and `scheduler.stop()` to grow a parameter (or split into `stop()` for full teardown vs. `pauseSchedules()` for "stop firing new jobs but let the in-flight ones finish").

### 2. Survive the scheduler instance being thrown away

Right now, the scheduler instance owns:

- `dedicatedJobControllers: Map<string, AbortController>` — keeps the controller alive for `runJobNow` cancellation
- `runningDedicatedJobs: Set<string>` — prevents duplicate firing while a job is in flight
- The unfinished `executeDedicatedJob` promise itself (held only by the `void` call site)

When `session_shutdown` fires, our `index.ts` discards the scheduler and constructs a new one in `session_start`. The unfinished promise is still rooted in the V8 heap (the timer callback chain that started it pins it), but the new scheduler has no handle to it. So:

- `runJobNow(jobId)` for a job that's still running in the OLD scheduler's promise won't see `runningDedicatedJobs` — it would happily fire a duplicate.
- The completing subprocess's continuation calls `notify(...)` and `captureRunRecordFromOutput(...)` on the old scheduler instance; the new scheduler never observes the completion.

The cleanest fix is to lift the in-flight tracking out of the per-scheduler instance:

- A module-scope `inFlightDedicated: Map<string, { controller, resultPromise }>` shared across scheduler lifetimes.
- The new scheduler's `start()` reads from this map to learn what's already running, suppress duplicate firing, and (optionally) re-attach completion handlers.

This is a notable architectural shift — the scheduler stops being a self-contained owner of process state — but it's the only way for "the scheduler instance was replaced" to be invisible to the subprocess.

### 3. Where do begin/end notifications go?

Today `notify()` calls `pi.sendMessage(...)` so the user sees `⏳ Processing begins…` / `✓ Processing ended.` lines in their session. After `/new`, the new session is a different conversation — should it receive end notifications about jobs that started in the previous session?

Three plausible answers:

- **Drop the chat notifications, keep the run record.** The user can still see results via `/replay`. This is the simplest and matches the "dedicated context = invisible to main session" intent. We just need `captureRunRecordFromOutput` to be called from somewhere that survives the scheduler swap (see #2).
- **Show end notifications in whatever session is current when the job finishes.** Surprises the user ("what's this scheduled prompt I never saw start?") and arguably re-couples dedicated jobs to the main session.
- **Queue the notification and replay it next time the originating context is active.** Most accurate, hardest to implement — there's no stable identity for "the session that started this job" once `/new` has replaced it.

The first option is probably right. It also dovetails with #2 cleanly: if the only thing the trailing completion code has to do is `storage.addRunRecord(...)`, the module-scope tracker doesn't need to know about extension APIs at all.

## Suggested implementation order

1. **Move run-record persistence ahead of the chat notification.** Today `captureRunRecordFromOutput(...)` runs *before* `notify("scheduled_prompt_end", …)` (`src/scheduler.ts:840-870`), so the disk record is already safe even when notify is suppressed. Verify that's robust under `stop()` mid-flight, then explicitly stop calling `notify("scheduled_prompt_end")` for jobs whose scheduler was already stopped — current patch already does this via the `stopped` flag.
2. **Differentiate `stop()` from `pauseSchedules()`.** Add a teardown mode: `stop({ reason: "shutdown" | "session-replace" })`. On `"session-replace"`, do not abort `dedicatedJobControllers`, do not clear `runningDedicatedJobs`. On `"shutdown"`, do everything as today.
3. **Lift in-flight state to module scope.** A `dedicatedRunRegistry` exported from `scheduler.ts` (or a new file) that maps `jobId → { controller, completion: Promise<RunRecord> }`. Both old and new scheduler instances read/write through it. `executeDedicatedJob` registers on entry, deregisters in `finally`. New scheduler's `start()` consults the registry to suppress re-firing of jobs already in flight.
4. **Make the subprocess detach from the parent session lifecycle.** Confirm that pi-mono's `pi.exec` doesn't auto-kill children when the parent's `AgentSession` disposes. If it does, we need a different spawn path (e.g. `child_process.spawn` with `detached: true`) — at which point we should ask whether `pi.exec` is still the right primitive for dedicated jobs at all.
5. **Drop `scheduled_prompt_end` chat output for jobs that span a session replacement.** Keep the run record; users replay via `/replay`. Decide whether `scheduled_prompt_begin` should also be hidden in this case (probably yes, since the corresponding end will never appear in the same session).

## Open questions

- Does `pi.exec` install a parent-death signal on the child? If yes, step 4 is not optional — even leaving the abort alone won't keep the subprocess alive across `/new`, because pi-mono may tear the child down on `AgentSession.dispose()` independently.
- Multi-client leader election (`leader.pid`) currently assumes only one scheduler holds leadership at a time. If we lift state to module scope but keep one scheduler instance per session, leadership is unchanged. If a future change runs schedulers across worker processes, the registry needs to be on disk.
- `pendingDedicatedRetries` and `runningDedicatedJobs` interact: a manual `/schedule-prompt retry` while a job is in flight queues a follow-up run. After `/new`, that intent is currently lost when the scheduler is replaced. Whether to persist it is a product decision — probably no, since retries are user-initiated.
- The `scheduled_prompt_begin` message is sent *before* the `await pi.exec(...)`, so it always lands in the originating session. Is that fine, or should it also be suppressed when a `/new` is imminent? (We can't see the future, so probably we have to live with the begin-but-no-end shape.)
