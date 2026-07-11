# Flywheel Mode B — Live Intraday Alerts: Deploy Plan (for approval)

> Status: **PLAN ONLY — nothing built or deployed.** Written 2026-07-11 for the
> operator to approve with full understanding before anything touches oldmac
> (the live-money machine). Based on a full read of the bhiksha live-loop
> architecture and the oldmac surface.

## What Mode B is

A **read-only** intraday loop running on oldmac during market hours that, once a
minute, checks IWM and SPY for a FLASH or TREND detector fire and pings your
Telegram within ~1 minute of the bar closing. **Signals only** — it places no
orders, touches no money, and shares no code with bhiksha's trading loop. It is
a second, isolated process whose only outputs are Telegram messages and a log.

## Monday, if approved — what you'd actually experience

- **08:20 CT** — the consult loop starts (same time bhiksha's trading loop
  starts), warms up ~60 sessions of IWM/SPY history for its baselines.
- **During the session** — each minute it fetches the just-closed 1-min bar for
  IWM and SPY, runs the detectors, and on a *new* fire that clears your
  thresholds, sends you a Telegram like:

  > ⚡ **IWM FLASH short** · 10:42 ET · 0.31-ATR flush into a fresh high · str 0.31
  > _signal only — your call. logged._

- **Cadence** — roughly **3–6 pings on a normal day**, sometimes 0 on a quiet
  one (your own thresholds are applied: flash ≥ 0.20 ATR, exhaustion ≥ p85,
  whipsaw pairs dropped). You glance, decide, and trade in your own account or
  don't. Nothing acts for you.
- **15:00 CT** — loop stops. The ledger has logged every fire (taken or not);
  your fills auto-match to fires within a day or two, feeding Phase C.

That's the product: a fast tap on the shoulder for your two mechanized
playbooks, on your two home symbols, with zero babysitting.

---

## THE decision that determines whether this is useful: data latency

The detectors are only as good as the freshness of the bars. There are two live
data paths on the box, and this is the real choice:

| | **Schwab** (bhiksha's feed) | **Polygon** (mala's feed) |
|---|---|---|
| Latency | **Real-time** (bhiksha trades real money on it) | **Likely 15-min delayed** on standard tiers — must be confirmed |
| Good for flash? | **Yes** — alert ~1 min after the move | **No** if delayed — a 15-min-old flash is useless |
| Isolation | Separate process, but reuses bhiksha's Schwab client + token file (refreshed by the existing schwab-guard job) | Fully isolated — mala's own Polygon key, zero bhiksha coupling |
| Extra load | IWM already polled by the trading loop; +1 symbol (SPY) per minute | Independent quota, no touch to the trading feed |

**Recommendation: Schwab (real-time).** Flash reversal is your fastest play; a
delayed feed defeats the purpose. The cost is a small, well-bounded coupling —
the consult process imports bhiksha's `SchwabBarSource` and reads the same
token file — but it stays a *separate read-only process* with no order code and
no PID collision with the trading runtime.

**Open question #1 for you / me to confirm:** is the Polygon key on oldmac a
real-time tier? If yes, the fully-isolated Polygon path becomes viable and we
avoid all bhiksha coupling. I can test this in 5 minutes on approval. Until
confirmed, the plan assumes Schwab.

---

## What gets built (new code — nothing existing is modified)

All new, in mala_v2 (both repos run Python 3.14 on oldmac — no cross-env issue):

1. **`src/research/playbook_detectors.py`** *(new, small)* — lift the 4 frozen
   detector functions (`flash_A`, `flash_C`, `trend_C`, `exh_C`) + thresholds
   out of `scripts/p2_detector_scorecard.py` into a proper importable module, so
   the live loop and the batch scripts share one definition (no drift). Pure
   refactor of already-tested code.

2. **`scripts/flywheel_live_consult.py`** *(new — the loop)* — a standalone
   long-running process that:
   - on start, warm-starts ~60 sessions of IWM/SPY 1-min history (for the
     ATR/stretch/multi-day baselines the detectors need) and builds the
     `SymbolBars` baseline;
   - each minute at :05, fetches the latest completed bar per symbol (via the
     chosen data source), appends it to today's session, evaluates the detectors
     on that bar only (causal — no lookahead), debounces (one alert per fire),
     and on a new fire sends the Telegram alert + appends to the fire ledger;
   - has its own PID guard (`consult.pid`) so it can't double-run and can't
     collide with `bhiksha.pid`;
   - runs 08:30–15:00 CT, then exits clean.
   Reuse points: bhiksha's `SchwabBarSource.fetch_latest_completed_bar` (if
   Schwab path) and the same Telegram delivery bhiksha uses.

3. **Telegram delivery** — reuse bhiksha's `ops.alerts.send_lathi_alert(...)`
   (profile `jarvis-northstar`, secrets already on the box at
   `~/.lane-host/secrets/`), OR the direct lathi-bus CLI call the existing
   `flywheel_daily.py` already uses. Either is a one-liner; no new Telegram
   integration.

4. **A launchd job pair** *(new)* — mirror bhiksha's `live-start` /
   `live-watchdog` pattern: a `consult-start` (08:20 CT) that launches the loop
   and a lightweight watchdog (every 10 min, 08:30–14:50 CT) that restarts it if
   it died. Two new plists.

No changes to any bhiksha trading code, the active plan, the Sheet, the risk
rails, or the exit profiles. The only shared surfaces are the Schwab token file
(read-only) and the Telegram channel.

---

## What gets deployed, and where

| Step | Where | Action |
|---|---|---|
| 1. Confirm data latency | oldmac | 5-min Polygon real-time test; decide Schwab vs Polygon |
| 2. Sync the new code | oldmac `~/Documents/mala_v2` | rsync/bundle the 2 new files + refactor (repo isn't pushed to a shared remote, so same transfer mechanism as before) |
| 3. Verify a dry run | oldmac | run the loop once against a recent day in `--dry-run` (prints alerts, sends nothing) to confirm it fires correctly on live-shaped data |
| 4. Install the launchd pair | oldmac `~/Library/LaunchAgents` | write + `launchctl bootstrap` the 2 plists (on-box step, like bhiksha's installer) |
| 5. First live day, supervised | oldmac | I watch the first session's log + your first few alerts, confirm cadence and latency, then hand off |

Nothing is irreversible: the loop can be `launchctl bootout`'d instantly, and it
never writes to any money-path surface.

---

## Safety implications (why this is low-risk despite touching oldmac)

- **No order path exists in the code** — it cannot trade, by construction.
- **Separate process, separate PID** — cannot interfere with or crash the
  trading runtime; the two never share memory or locks.
- **Read-only on shared surfaces** — it reads the Schwab token file and writes
  only to its own ledger + the Telegram channel.
- **Provider load** — if Schwab: +1 symbol/minute (SPY; IWM already polled),
  well within the existing concurrency cap. If Polygon: fully independent quota.
- **The one thing to watch** — the Telegram channel is shared with bhiksha's
  session reports; the alerts are tagged distinctly so they don't get confused
  with trading-system messages.

## Open questions for you (the approval gate)

1. **Data source** — confirm real-time is required (I assume yes for flash), and
   let me test the Polygon tier; if it's delayed, we go Schwab.
2. **Playbooks in scope** — FLASH + TREND are the two that passed P2/P3.
   Include the EXHAUSTION screen too (flagged as lower-confidence), or hold it
   until Phase C sharpens it? My rec: FLASH + TREND live, EXHAUSTION off at first.
3. **Alert volume tolerance** — 3–6/day is the estimate on current thresholds.
   Want it tighter (fewer, higher-conviction only) from day one?
4. **Green-light to touch oldmac** — steps 2–5 above run on the live machine.

## Effort & sequencing

- Build + local test: ~half a day (the detectors and feature code already exist;
  this is a loop wrapper + a data-fetch adapter + 2 plists).
- Deploy + supervised first day: ~1 session.
- **This does NOT block Phase C** (execution/exit-alpha measurement), which is
  pure offline analysis on your existing fills and needs no oldmac and no
  approval — I can run that in parallel or first, your call.

## What I need from you to proceed

A yes on the four open questions above (at minimum: data-source confirmation and
the green-light to touch oldmac). On approval I'll: confirm the Polygon tier,
build the two files + refactor, dry-run locally, then walk you through the
oldmac deploy step by step before the first live session.
