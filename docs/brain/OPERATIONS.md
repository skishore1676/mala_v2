---
as_of: 2026-07-09
sources:
  - docs/LIVE_LOOP_WORKPLAN.md  # cadence spec + dated diary (mala_v2)
  - .supervisor-lane/STATE.md   # worker access idioms, worktree lesson
  - bhiksha/src/bhiksha/ops/launchd_registry.py  # the 7 jobs (verbatim)
  - bhiksha/scripts/launchd/{install_bhiksha_launchd.sh, run_bhiksha_job.sh}
  - bhiksha/src/bhiksha/tools/launchd_job.py  # manual-run CLI + alert profiles
  - bhiksha/src/bhiksha/ops/alerts.py  # send_lathi_alert / send_obsidian_review defaults
  - bhiksha/src/bhiksha/tools/compile_active_plan.py  # active_strategies tab
  - bhiksha/docs/lessons/{money-path-audit-rounds..., sheet-is-the-operator-control-surface}.md
  - ~/.claude/skills/lathi-review-bus/SKILL.md
  - ~/.claude/projects/-Users-suman-code-mala-v2/memory/ (live-experiment-status-2026-07, companion-stack-hermes-pulsar-beacon)
  - docs/RESIDENT_INTELLIGENCE_RFC.md §5.2/5.3/5.6
---

# OPERATIONS — how to run this system

Runbook truth for a fresh agent. **Trust order: runtime evidence > diary > this file.**
If this file's `as_of` looks stale, re-verify against the runtime before acting.
Safety posture (RFC §5.6): the brain is **read-and-recommend** — no Sheet writes, no
deploys, no order-path anything. Reflection/watch jobs run read-only.

## Machines

- **Dev machine (usually this Mac):** `/Users/suman/code/{mala_v2,bhiksha,kernel,lathi-bus}`.
- **oldmac = the live runtime host.** bhiksha runtime checkout is
  `/Users/sunny/Documents/bhiksha` — **NOT `~/code`** (a common trap). Its lathi-bus
  checkout is `/Users/sunny/code/lathi-bus`.
- **ssh to oldmac is read-only for agents.** Query state (sqlite, logs, git log); never
  write files, never run the trading-runtime jobs, from a watch/steward. Deploys are the
  one deliberate exception, done by-hand at the session boundary (below).

## Operating cadence (mala_v2, operator-agreed 2026-07-02, ~2 weeks)

Trading days, CT:
- **09:42 morning watch** — quiet unless notable.
- **13:07 midday watch** — quiet unless notable.
- **15:24 close readback** to operator **+ ONE build increment/day** from the status board.
- **Fridays: weekly synthesis instead of a build.**
- **Every wake-up appends a dated Diary entry** to `docs/LIVE_LOOP_WORKPLAN.md` (watches
  add a line or two; the close entry is the full one). The close job **commits that doc
  in mala_v2 locally**.

**mala_v2 is deliberately NOT pushed.** Verified 2026-07-09 with `git status -sb`:
`## main...origin/main [ahead 36]` — 36 local commits, unpushed. The local-only cadence is
what keeps the live diary/edge off public GitHub (both repos are public). Keep it that way:
commit locally, do not `git push` mala_v2 unless the operator says so.

The **watch/build cron jobs are session-scoped on the dev Mac** (expire ~2026-07-13, do not
survive a Claude restart) — NOT launchd. If they vanish, tell Claude "resume the trading
cadence"; the cadence spec above is how to recreate them. The 7 launchd jobs + rails below
are the deterministic safety layer and run on oldmac independently of the cadence session.

## The 7 launchd jobs (oldmac)

Source of truth: `bhiksha/src/bhiksha/ops/launchd_registry.py`. All times CT.

| Label | Schedule | Purpose |
|---|---|---|
| `com.bhiksha.live-start` | Weekdays 08:20 | Restart the live runtime from the active plan. |
| `com.bhiksha.live-watchdog` | Weekdays every 10 min, 08:30→15:00 | Ensure the live runtime is still running. |
| `com.bhiksha.live-stop` | Weekdays 15:10 | Stop the runtime so stale processes do not survive close. |
| `com.bhiksha.schwab-guard` | Weekdays 07:10 | Schwab token guard; browser re-auth only when needed. |
| `com.bhiksha.session-report` | Weekdays 09:10 / 11:45 / 14:45 | Intraday Telegram report, early enough for manual action. |
| `com.bhiksha.weekly-scorecard` | Fridays 15:20 | Publish the weekly profile-vs-legacy scorecard (the month-test verdict). |
| `com.bhiksha.shadow-ev-report` | Weekdays 15:30 | Daily shadow-EV report: which shadow lanes are earning promotion. |

The three `live-*` jobs are `risk_class=trading_runtime` and carry
`requires_confirmation_actions` — do not casually invoke them. `weekly-scorecard` and
`shadow-ev-report` are the two added in the 2026-07-09 backlog fold-in.

### Run a job manually (on oldmac)

```bash
# runner names: live-start live-watchdog live-stop schwab-refresh
#               session-report weekly-scorecard shadow-ev-report
bash scripts/launchd/run_bhiksha_job.sh weekly-scorecard        # wraps python -m bhiksha.tools.launchd_job
# python -m bhiksha.tools.launchd_job shadow-ev-report --force  # --force runs on a non-trading day
#   --alert-mode spool  → build the artifact but do NOT deliver (safe dry preview)
```

### Logs & install

- **Logs (oldmac):** `~/Documents/bhiksha/artifacts/playbook/launchd/<label>.out.log` and
  `<label>.err.log`; last run snapshot in the same dir at `latest_status.json`.
- **Install/uninstall:** `scripts/launchd/install_bhiksha_launchd.sh [install|uninstall]`
  (run ON oldmac) — writes plists to `~/Library/LaunchAgents` and
  `launchctl bootstrap gui/$uid`s all 7 labels. After any deploy that touches the registry,
  re-run install and confirm all 7 are loaded.

## sqlite access idioms

- **On the live host:** `sqlite3 -cmd ".timeout 8000" <db>` — the busy-timeout keeps a read
  from erroring while the live writer holds a lock.
- **A snapshot pulled from a live-writer host:** open with `immutable=1`
  (`file:/path/db.sqlite?immutable=1`) so a concurrent writer can't corrupt your read. This
  is the standing worker convention in `.supervisor-lane/STATE.md`.

## bhiksha worktree testing (the gotcha)

Bare `PYTHONPATH=src` **fails** in a bhiksha worktree. Use the **MAIN checkout's venv python
plus kernel src on PYTHONPATH**:

- python: `/Users/suman/code/bhiksha/.venv/bin/python`
- worktrees live under `/Users/suman/code/bhiksha-worktrees/`
- kernel symlink: `/Users/suman/code/bhiksha-worktrees/mala-bhiksha-kernel` →
  `/Users/suman/code/mala-bhiksha-kernel` (verified). Put its `src` on PYTHONPATH alongside
  the worktree's `src`.
- Known-environmental: `test_runtime_snapshot` may fail in a worktree — confirm it also
  fails at clean `main` before treating it as a regression.

## Brain steward (nightly, DEV Mac — RFC 9a Q1 + Q2-REVISED)

`com.mala.brain-steward` (launchd, dev Mac, 21:45 local) runs
`scripts/brain/steward.py`: gathers evidence deterministically (mala git log, diary
tail, supervisor-lane tail, bhiksha logs, **read-only** oldmac ssh readback), hires a
**text-only** model via agent-broker (`scripts/brain/steward_policy.yaml`,
opus→sonnet→codex), validates the returned file blocks fail-closed (only
`docs/brain/STATE.md` + `docs/brain/candidates/*.md` accepted), auto-commits, runs
`scripts/brain/freshness_lint.py`, and on Fridays publishes an advisory curation digest
to the Obsidian Inbox via Lathi bus. On ANY failure it touches nothing — canon ages
visibly (stale beats wrong). It lives on the dev Mac because mala_v2 is unpushed and
canon lives here (RFC 9a Q2-REVISED); it never writes to oldmac.

```bash
python3 scripts/brain/steward.py --dry-run          # build the task bundle, no hire
python3 scripts/brain/steward.py                    # full run (hire + auto-commit)
python3 scripts/brain/freshness_lint.py             # staleness check (STATE budget 48h)
bash scripts/brain/install_brain_steward.sh status  # job loaded? + log tail
# logs + task bundles + receipts: ~/Library/Logs/mala-brain-steward/
```

Curation: steward commits are reviewed via the Friday digest card (pointy-bracket
comments prune/correct after the fact); `docs/brain/candidates/` holds drafts for
DECISIONS/ARCHITECTURE — the steward never edits those files directly.

## Lathi bus (alerts + phone review)

Contract: `~/.claude/skills/lathi-review-bus/SKILL.md`. The bus CLI **cwd-switches to the
lathi-bus checkout** — always pass **ABSOLUTE source paths** (relative paths broke the
Obsidian projection, a bug caught only on the oldmac runtime, 2026-07-09).

- **Telegram alerts:** `send_lathi_alert(...)`, default profile **`jarvis-northstar`**
  (`bhiksha/src/bhiksha/ops/alerts.py`, `tools/launchd_job.py`; env `BHIKSHA_LATHI_PROFILE`).
  `jarvis-northstar` is a **legacy profile NAME only** — delivered by the **Beacon** Telegram
  bot; the Jarvis agent is deprecated. Do not rename it.
- **Obsidian review cards:** `send_obsidian_review(...)`, default profile
  **`coding-agent-northstar`** → vault folder **`07 Agents/Coding/Inbox`** (Archive on close;
  machine packets under `_system/lathi-bus/coding-agent`). Session reports also project here
  (env `BHIKSHA_SESSION_REPORT_OBSIDIAN_MODE`, default on). **Do not write into the vault
  directly** — publish/collect/archive via the bus CLI from the lathi-bus dir; review with
  `<pointy-bracket>` comments.

## Deploy protocol

1. **Build / test / audit during the day** in isolated worktrees. Do NOT deploy while the
   live session is open.
2. **Deploy only after the session is hard-flat** (~15:15 CT, after the 15:10 `live-stop` /
   15:55 ET close). **One build increment per day** so the live experiment stays attributable.
3. **Money-path changes** (order path, exit ladder, entry/evidence gates) require
   **adversarial audit ROUNDS before merge** — a green suite is NOT proof (four consecutive
   round-2 same-auditor passes each caught the worst bug this week). The rule lives in
   `docs/brain/DECISIONS.md`; standing precedent:
   `bhiksha/docs/lessons/money-path-audit-rounds-catch-different-bug-classes.md`.
4. **Readback after EVERY deploy:** pull on oldmac, confirm `oldmac == origin == local`
   commit, boot green, the startup event shows the expected config (e.g. rails 7.5/11.25),
   and all 7 launchd jobs are loaded. State "verified against runtime", not "merged".

## Operator surfaces

- **Google Sheet — `active_strategies` tab (the arming surface).** Per-lane execution/exit
  cells (`authorization_mode`, `activation_candidate`, evidence-gate cells, profile-exit
  arm) drive the compiled plan (`bhiksha/src/bhiksha/tools/compile_active_plan.py`, env
  `ACTIVE_STRATEGIES_SHEET_NAME`). The compiler **honors and surfaces** gate keys — it never
  strips them (`bhiksha/docs/lessons/sheet-is-the-operator-control-surface.md`). Sibling tabs:
  `Mala_Evidence_v1` (catalog), operator-defaults (risk knobs), manual setups. Changing a
  risk dial in the Sheet appears in the morning report without any code change.
- **Telegram** — 3×/day session reports + Friday weekly scorecard + daily shadow-EV, via the
  Beacon bot. The primary always-on operator channel.
- **Obsidian Inbox** (`07 Agents/Coding/Inbox`) — review/approve/archive surface; session
  reports and review cards land here for phone review.
