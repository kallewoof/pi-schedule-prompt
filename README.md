# pi-schedule-prompt

A "Heartbeat" like prompt scheduling [Pi](https://pi.dev) extension that allows the Agent to self-schedule future prompts to execute at specific times or intervals - for reminders, deferred tasks, and recurring automation.


<img width="600"  alt="image" src="https://github.com/tintinweb/pi-schedule-prompt/raw/master/media/screenshot.png" />




https://github.com/user-attachments/assets/8c723cc4-cf3e-4b6a-abf5-85d4f46c73ba

> **Status:** Production-ready. Natural language scheduling with cron expressions, intervals, relative time, and one-shot timers.

Schedule future prompts with natural language:
- **"schedule 'analyze logs' every hour"** (recurring)
- **"remind me to review PR in 30 minutes"** (one-time)
- **"defer that task until tomorrow at 9am"** (specific time)

## Features

### Core `schedule_prompt` Tool
- **Natural language scheduling**: "schedule X in 5 minutes", "every hour do Y"
- **Multiple formats**: Cron expressions, intervals, ISO timestamps, relative time (+5m, +1h)
- **Job types**: 
  - **Recurring** (cron/interval) — repeats automatically
  - **One-shot** (once) — runs once then auto-disables
- **Actions**: add, remove, list, enable, disable, update, cleanup
- **Auto-cleanup**: Removes disabled jobs on session exit

### Use Cases

#### Schedule (Recurring Tasks)
Execute prompts repeatedly at set intervals:
```
"schedule 'check build status' every 5 minutes"
"run 'analyze metrics' every hour"
"execute 'daily summary' at midnight every day"
```

#### Remind (One-time Notifications)
Get prompted to do something once at a specific time:
```
"remind me to review the PR in 30 minutes"
"remind me to check deployment status in 1 hour"
"remind me tomorrow at 9am to follow up on the issue"
```


### Enhanced Pi Features
- ✓ **Live widget** below editor showing active schedules (auto-hides when empty)
- ✓ **Human-readable display**: "every minute", "daily at 9:00" instead of raw cron expressions
- ✓ **Status tracking**: next run, last run, execution count, errors, prompt preview
- ✓ **Flexible scheduling**: 6-field cron, intervals (5m, 1h), relative time (+10s), ISO timestamps
- ✓ **User commands**: `/schedule-prompt` interactive menu with widget visibility toggle
- ✓ **Safety features**: duplicate name prevention, infinite loop detection, past timestamp handling
- ✓ **Context-aware routing**: in multi-context RPC mode, jobs fire back into the conversation that created them (e.g. the Signal thread or group)

### Context-aware routing (multi-context RPC mode)

When pi runs in `--mode rpc` with named context sessions (used by bridges like [pi-signal-messenger](https://github.com/kallewoof/pi-signal-messenger) where each Signal thread or group is a separate context), `schedule_prompt` automatically captures `ctx.context` at job-creation time and stores it on the job as `targetContext`.

When the job fires, the scheduler delivers the prompt back into that same context session via `pi.sendUserMessageToContext` / `pi.sendMessageToContext` instead of the default session. This keeps each conversation's scheduled prompts (and their replies) isolated to the thread that requested them.

Falls back to the default session when:
- The pi runtime does not expose context routing (older versions, non-RPC modes)
- `ctx.context` is `undefined` (job was created from the main session)

No user action is needed — the routing is transparent.

## Install

**Option A — Install from npm:**
```bash
pi install npm:pi-schedule-prompt
```

**Option B — Load directly (dev):**
```bash
pi -e ~/projects/pi-cron-schedule/src/index.ts
```

**Option C — Install from local folder:**
```bash
pi install ~/projects/pi-cron-schedule
```

Then run `pi` normally; the extension auto-discovers.

## Usage

### LLM-driven (automatic)

The agent automatically uses `schedule_prompt` when you want to schedule, defer, or be reminded:

```
You: Remind me to check the deployment logs in 10 minutes

Agent: [calls schedule_prompt with schedule="+10m", prompt="check the deployment logs"]
✓ Scheduled job "abc123" to run in 10 minutes
```

The widget displays below your editor (only when jobs exist):

```
 Scheduled Prompts (3 jobs)
  ✓ check-logs    every hour      check deployment logs     in 45m    12m ago  5
  ✗ daily-report  daily           analyze metrics           in 8h     never    0
  ✓ review-pr     Feb 13 15:30    review PR #123            in 2h     never    0
```

### Manual commands

| Command | Description |
|---------|-------------|
| `/schedule-prompt` | Interactive menu: view/add/toggle/remove jobs, cleanup, toggle widget visibility |

### Schedule Formats

The tool accepts multiple time formats:

| Format | Example | Type | Description |
|--------|---------|------|-------------|
| **Relative time** | `+5m`, `+1h`, `+30s`, `+2d` | once | Runs once after delay |
| **Interval** | `5m`, `1h`, `30s`, `2d` | interval | Repeats at interval |
| **ISO timestamp** | `2026-02-13T15:30:00Z` | once | Runs once at exact time |
| **Cron expression** | `0 */5 * * * *` | cron | Runs on cron schedule |

**Cron format** (6 fields - **must include seconds**):
```
┌─ second (0-59)
│ ┌─ minute (0-59)
│ │ ┌─ hour (0-23)
│ │ │ ┌─ day of month (1-31)
│ │ │ │ ┌─ month (1-12)
│ │ │ │ │ ┌─ day of week (0-6, Sun-Sat)
│ │ │ │ │ │
0 * * * * *   → every minute
0 0 * * * *   → every hour
0 */5 * * * * → every 5 minutes
0 0 0 * * *   → daily at midnight
0 0 9 * * 1-5 → 9am on weekdays
* * * * * *   → every second
```

**Note:** Traditional 5-field cron expressions (without seconds) are not supported. Use `0 * * * * *` for "every minute", not `* * * * *`.

## How It Works

**Storage:**
- File-based persistence at `.pi/schedule-prompts.json` (project-local)
- Atomic writes prevent corruption
- Auto-creates directory structure

**Scheduler:**
- Uses `croner` library for cron expressions
- Native `setTimeout`/`setInterval` for intervals and one-shots
- Tracks: next run, last run, execution count, status (running/success/error)

**Execution:**
- Sends scheduled prompt as user message to Pi agent
- Displays custom message showing what was triggered
- Updates job statistics after each run

**Safety:**
- **Infinite loop prevention**: Blocks scheduled jobs from creating more schedules
- **Past timestamp detection**: Auto-disables jobs scheduled in the past
- **Duplicate names**: Prevents name collisions
- **Auto-cleanup**: Removes disabled jobs on exit

**Widget:**
- Auto-hides when no jobs configured
- Shows: status icon, name, schedule (human-readable), prompt (truncated), next run, last run, run count
- Human-readable formatting: "every minute", "daily", "Feb 13 15:30" instead of raw cron/ISO
- Auto-refreshes every 30 seconds
- Toggleable visibility via `/schedule-prompt` menu
- Status icons: `✓` enabled, `✗` disabled, `⟳` running, `!` error

## Examples

### One-time reminders
```
"remind me to check logs in 5 minutes"
  → schedule="+5m", type=once

"schedule 'review metrics' for 3pm today"
  → schedule="2026-02-13T15:00:00Z", type=once
```

### Recurring tasks
```
"analyze error rates every 10 minutes"
  → schedule="10m", type=interval

"run daily summary at midnight"
  → schedule="0 0 0 * * *", type=cron

"check build status every hour"
  → schedule="0 0 * * * *", type=cron

"execute every minute"
  → schedule="0 * * * * *", type=cron
```

### Heartbeat monitoring
```
"check system health every 5 minutes"
  → schedule="5m", type=interval
```

## Development

**TypeScript check:**
```bash
npx tsc --noEmit
```

**Test with Pi:**
```bash
pi -e ./src/index.ts
```

## Project Structure

```
src/
  types.ts          # CronJob, CronJobType, CronToolParams
  storage.ts        # File-based persistence (.pi/schedule-prompts.json)
  scheduler.ts      # Core scheduling engine with croner
  tool.ts           # schedule_prompt tool definition
  ui/
    cron-widget.ts  # Live status widget below editor
  index.ts          # Extension entry point
```

## License

MIT (see [LICENSE](LICENSE))

## Author

[tintinweb](https://github.com/tintinweb)
