# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a [Pi](https://pi.dev) coding agent extension written in TypeScript. It adds a `schedule_prompt` tool that lets the agent autonomously schedule future prompts using cron expressions, ISO timestamps, relative times (`+10m`), or simple intervals (`5m`, `1h`).

## Commands

```bash
# Type check
npx tsc --noEmit

# Run locally with Pi
pi -e ./src/index.ts
```

The package is published to npm as `pi-schedule-prompt`. There is no separate build step for local development — Pi loads TypeScript directly via `-e`.

## Architecture

**Entry point:** `src/index.ts` — registers the extension lifecycle events (`session_start`, `session_switch`, `session_fork`, `session_shutdown`), the `schedule_prompt` tool, and a `/schedule-prompt` CLI command.

**Core modules:**

- `src/types.ts` — shared type definitions (`CronJob`, `CronJobType`, `CronStorage`, etc.)
- `src/storage.ts` — file-based persistence at `.pi/schedule-prompts.json` with atomic writes (temp file → rename)
- `src/scheduler.ts` — scheduling engine wrapping the `croner` library for cron jobs and native `setTimeout`/`setInterval` for one-shot and interval jobs; calls back into the Pi session to send the prompt as a user message
- `src/tool.ts` — defines the `schedule_prompt` tool schema (TypeBox) and its `add`/`remove`/`list`/`enable`/`disable`/`update`/`cleanup` actions
- `src/ui/cron-widget.ts` — Pi TUI widget rendered below the editor, auto-refreshes every 30 seconds, hidden when no jobs exist

**Data flow:** `session_start` → load storage → start scheduler → on tool call: persist job → schedule it → on fire: inject user message → storage updated with last-run time.

**Safety invariants to preserve:**
- Recursive scheduling is blocked: prompts injected by the scheduler cannot themselves call `schedule_prompt`
- Past timestamps are rejected at tool-call time
- Duplicate job names are rejected
