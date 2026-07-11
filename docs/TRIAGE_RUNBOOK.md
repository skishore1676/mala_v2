# Triage Runbook — how to sweep symbols/strategies to shadow-ready candidates

> **Audience:** an agent (or operator) who must triage a NEW symbol, a new symbol-batch, or an
> existing strategy family into shadow-ready candidates. This is the reusable how-to distilled from
> the 2026-07-10 Complete Triage Program (`docs/COMPLETE_TRIAGE_PROGRAM.md` is that program's record;
> this is the repeatable procedure). Companion: `docs/lessons/triage-publication-gates-what-actually-blocks.md`.

## What "done" is
A candidate is shadow-ready when it clears ALL of:
1. **Multi-regime survival** — passes holdout in the recent era AND ≥1 prior regime era, **same
   direction** (deploy direction must have survived a prior regime — not just "some config passed").
2. **Validated yardstick** — `recommendation_tier ∈ {shadow,promote}` (encodes `mc_prob≥0.70`,
   `base_exp_r>0`, `holdout_trades≥15`). Win-rate/payoff are **context, not a hard gate** (see Landmine 7).
3. **Profile-exit option-path > 0** with a mapped `management_policy_spec`.
4. **`bhiksha_capability_status = supported`**.
5. **M7 provider parity ≥ 0.90** (Schwab-vs-Polygon signal overlap, computed from a real oldmac fetch).
6. A staged `Mala_Evidence_v1` row (local) → operator publish.

The ONLY human stop is the publish. Everything else is driver-owned and reversible.

## The pipeline (scripts, in order)
All runs are LOCAL on the Mac Air (10 cores). **Never run compute on oldmac** — it's the production
box (4 cores under the live loop). oldmac is used ONLY for the Schwab M7 fetch and manifest pulls.

```
# 0. One-time: sync FULL-history bars from oldmac (see Landmine 1 & 2)
rsync -a --prune-empty-dirs --exclude='download_logs' --exclude='live_feedback' --exclude='results' \
  --exclude='iv_snapshots' --exclude='personal_imports' --exclude='examples' --exclude='replay_workbook' \
  "oldmac:Documents/mala_v2/data/" ./data/

# 1. WAVE — multi-regime sweep (family × symbol × 3 eras → M4 holdout)
./.venv/bin/python scripts/triage_wave.py --families trend --universe all --workers 5 --wave-name w1_trend
#   families: trend (MI+OD+JP) | reversion (elastic_band) | range (compression=research-only) | all
#   --symbols A B C   to target a batch instead of --universe all

# 1b. RESCAN authoritative (ALWAYS after a wave; the running process used pre-edit code)
./.venv/bin/python scripts/triage_wave.py --rescan w1_trend --rescan-families trend   # MUST pass families (Landmine 8)

# 2. ACCEPTANCE — deployable(e3) + robust(≥1 prior era), direction-aware terrain + survivors.csv
./.venv/bin/python scripts/triage_acceptance.py --manifest data/results/triage_waves/w1_trend__manifest.csv

# 3. TIER-B — recent-window M5 + option-path + yardstick + FDR (per survivor)
./.venv/bin/python scripts/triage_tierb.py --survivors data/results/triage_waves/w1_trend__survivors.csv --workers 6

# 4. DIRECTION-CONSISTENCY — drop candidates whose deploy direction never survived a prior regime
./.venv/bin/python scripts/triage_dir_consistency.py --tierb data/results/triage_tierb/w1_trend__tierb.csv

# 5. PROFILE-EXIT scoring (the REAL option path bhiksha uses — IV band, native profile exit)
#    Feed the dir-robust survivors' Tier-B run dirs (they contain CATALOG_SELECTED.csv):
./.venv/bin/python scripts/classify_explore_propose.py --run-dir <tierb_run_dir> [--run-dir ...] --out-dir <dir>

# 6. CAPABILITY — pull manifest from oldmac, evaluate (see Landmine 6 for the exit-policy PREFIX)
scp oldmac:Documents/bhiksha/artifacts/capabilities/bhiksha_runtime_capabilities_v2.json data/bhiksha_manifests/

# 7. M7 PARITY — fetch ~1 month (21 trading days) Schwab bars on oldmac, measure signal overlap
ssh oldmac 'zsh -lc "cd ~/Documents/bhiksha && ./.venv/bin/python -m bhiksha.tools.provider_divergence --symbol QQQ --trading-days 21 --csv /tmp/m7_QQQ.csv"'
#   then compute stack/stretch-sign overlap locally (see M7 section). ≥0.90 = provider_pass.

# 8. STAGE — assemble publish-ready Mala_Evidence_v1 rows + review packet (NO sheet write)
./.venv/bin/python scripts/triage_stage_rows.py --candidates fam:sym:dir ... --m7 <m7.csv> --classify-dir <dir> --wave w1_trend
```

## The gate definitions that matter
- **Regime eras** (`triage_wave.ERAS`): e1_bear2022 (holdout 2023 H1), e2_bull2324 (holdout 2024),
  e3_recent (holdout 2025-12→2026-02). Each calibration span ≥18mo so M1 yields ≥3 walk-forward windows.
- **e3_recent = the deployment gate** (recent calib→holdout, mirrors how live lanes were promoted).
  e1/e2 = robustness. Survivor = deployable AND robust, **same direction**.
- **Yardstick** = the operator's own pipeline floor: `mc_prob≥0.70`, `base_exp_r>0`, `holdout_trades≥15`.
  win 0.45–0.62 / payoff>1 are the convexity-DNA *context*, reported not enforced.
- **M7**: `config/m7_provider_translation.yaml` — `activation_min=0.90`, `block_below=0.80`, red feature
  risk blocks. Runtime provider is Schwab (`oldmac:bhiksha/config/providers.yaml`).

## LANDMINES (every one of these bit us — do not re-learn them)
1. **`rsync --files-from=<symbol-list>` creates EMPTY dirs** (no recursion). Use the recursive form
   in the pipeline above.
2. **This Mac's originally-cached symbols were truncated to 2024–2026.** Full 2021→ history only
   comes from the oldmac pull. Multi-regime needs full history — verify `first=2021-*` per symbol.
3. **A running wave uses the code it started with.** If you edit `triage_wave.py` mid-run, the wave's
   own final manifest is stale — ALWAYS `--rescan` afterward to get authoritative results.
4. **Pass = the funnel's OWN decision, not your reconstruction.** Early harness read `passes_cost_gate`
   itself and over-counted (a positive `combined` config looked like a pass while the funnel said
   `M4: 0 promoted → kill`). Read `promote_to_m5`/`M4 promoted>0` from RUN_SUMMARY.md.
5. **"Multi-regime robust" must be DIRECTION-consistent.** The killer bug: a symbol can pass all 3
   eras with LONG winning e1/e2 and SHORT winning e3 — then you recommend SHORT on "3/3 robust" that
   is actually recent-only. `triage_dir_consistency.py` fixes this; never skip it.
6. **Capability keys on the exit-policy PREFIX** (`policy.split(":")[0]`). Passing the full
   `ma_crossover_underlying:ema_12>ema_50` string → false `unsupported`. The legacy underlying exits
   (fixed_rr/atr_trailing/time_stop/ma_crossover_underlying) ARE all in `supported_thesis_exit_policies`.
7. **win-rate/payoff are NOT the operator's gate.** His `recommendation_checks_json` has neither.
   Gating hard on them is stricter than his accepted candidates and drops good names (e.g. SNOW short,
   win 0.68). Report them; don't reject on them.
8. **`--rescan` globs ALL cells in OUT_ROOT** (waves share the dir). Pass `--rescan-families` or you'll
   mix W1 trend cells into the W2 reversion manifest.
9. **Full-window calibration (2021→2025) over-penalizes.** Even live-good NVDA dies at M4 on it — the
   2022 regime drags configs off the current one. The DEPLOYMENT gate is the RECENT window (e3).
10. **Foreground bash timeout kills backgrounded children.** Launching a long run with `nohup ... &`
    inside a Bash call whose `until`-loop then times out (exit 143) SIGTERMs the process group and
    kills the run. Use the tool's `run_in_background: true` for long runs.
11. **`classify_explore_propose` is slow** (per-symbol bar enrichment) — >2min for ~8 symbols. Run in
    background; or reuse an existing classify INDEX that already covers the symbols.

## M7 signal-overlap, computed (the volume trap)
Schwab vs Polygon: **price parity is ~perfect** (0–5 divergent bars/7800) but **raw volume diverges**
(~16% median definition/scale offset). This is NOT fatal:
- **market_impulse** keys on VWMA, which is invariant to a volume scale factor → measure VWMA-stack
  state agreement. Observed 95–98% → pass.
- **opening_drive** is price-driven → price EMA-stack agreement 99.9% → pass.
- **elastic_band** keys on stretch-from-mean → sign(close − VWMA20) agreement 98–99% → pass.
Compute the appropriate per-family proxy from the `provider_divergence` CSV; require ≥0.90.

## NON-blockers (do not sink time here)
- **IV validation** — bhiksha uses NO IV at runtime (executes on price/R/time). Modeled IV only ranks
  backtests; shadow accrues real IV. Not a gate.
- **Adversarial disprove pass** — runs pre-PROMOTE, after shadow. Not required to publish a shadow row.
- **3 known funnel bugs** (logged in the program doc): funnel never calls `score_profile_band`;
  `mc_prob` bootstraps the base holdout set not the selected exit; `use_real_iv` collapses the adverse-IV
  band. Real research-scorer defects, but they don't block shadow (they argue for it).

## Publish (the only stop point)
- Writing `Mala_Evidence_v1` is ADVISORY. Verified: no cron/launchd auto-runs
  `sync_google_strategy_catalog`/`compile_active_plan`, so a sheet write does NOT cascade to
  `active_strategy` or live. Deploy is a separate explicit operator compile.
- Snapshot the sheet to CSV first (rollback). Append rows with a `triage-` catalog_key prefix so they
  are identifiable/removable. NEVER write `active_strategy` without the operator.
- The sheet is Mala-owned and regenerated by `mala_handoff`; hand-appended rows may be overwritten by a
  future full handoff run — note this when appending.
