# Classical Pattern Lab — Semantic Review Round 1

Status: local semantic pilot ready; review decisions pending
Date: 2026-07-17
Playbook: `classical-rectangle-breakout-daily@1`
Readiness: semantic calibration only; not economic or executable

## What This Round Proves

This round asks one narrow question before any backtest claim:

> At the close shown on each chart, did the deterministic machine identify a
> genuine horizontal rectangle, faithful boundaries, a completed breakout,
> and the correct Last Full Day risk reference?

The packet contains ten 2022 signals: five long and five short, spread across
ten symbols and all four quarters. The cohort is deliberately enriched for
positive detector signals. It can measure candidate precision and anchor
fidelity; it cannot measure false negatives or recall.

## Data Boundary

The source window is 2021-09-01 through 2022-12-31 so January 2022 signals have
prehistory. Review eligibility is 2022-01-01 through 2022-12-31.

- 23 audited symbols.
- All 251 expected 2022 NYSE sessions present for every cohort symbol.
- Zero duplicate timestamps or invalid/null/non-finite OHLCV failures.
- Calendar-aware handling retains the six valid 2022–2024 13:00 ET closes that
  the earlier fixed 300-bar rule discarded.
- Provider and adjustment provenance remain unverified at file level.
- The cache is a present-day symbol collection, not a point-in-time universe.

Therefore the cache is suitable for this local blind semantic pilot, but not
for alpha, robustness, or promotion claims. NYSE's published calendar defines
regular early closes at 13:00 ET: <https://www.nyse.com/trade/hours-calendars>.

## Outcome-Hidden Contract

The generator imports the causal enumerator and daily bars only. It does not
read lifecycle outcomes, trades, P&L, MFE/MAE, economic reports, or the prior
backtest receipt.

Each card contains:

- bars ending exactly at the close-confirmed signal;
- central boundaries and the tolerance envelope;
- causal touch markers;
- highlighted breakout bar and Last Full Day;
- geometry facts available at the cutoff;
- an editable response row keyed by config, card, chart, and source hashes.

The review chart deliberately omits structural objective, subsequent bars,
trade fills, and all economic results. Sampling uses direction, lookback,
geometry band, as-of diagnostic band, symbol spread, and a fixed hash seed—no
future result.

## Current Review Packet

Generated root:

```text
research/results/playbooks/classical_pattern_lab/semantic_round_1/
  readiness/
    DATA_READINESS.md
    data_readiness.json
  review_batch_2022_v1/
    REVIEW_INDEX.md
    batch_manifest.json
    batch_receipt.json
    review_responses.template.csv
    review_responses.csv
    cards/
    charts/
```

Only `review_responses.csv` is editable. The immutable template remains hashed
so the response sheet can always be restored without changing the batch.

## Regeneration

```bash
symbols="SPY,QQQ,IWM,DIA,GLD,TLT,AAPL,MSFT,NVDA,AMZN,META,TSLA,JPM,XOM,UNH,WMT,ABNB,AMAT,AVGO,CRM,GOOGL,GS,XLE"
batch_root="research/results/playbooks/classical_pattern_lab/semantic_round_1/review_batch_repro_$(date -u +%Y%m%dT%H%M%SZ)"

uv run --frozen python -m src.research.classical_patterns.runner audit-cache \
  --symbols "$symbols" \
  --start 2021-09-01 \
  --end 2022-12-31 \
  --output-dir research/results/playbooks/classical_pattern_lab/semantic_round_1/readiness

uv run --frozen python -m src.research.classical_patterns.runner semantic-batch \
  --symbols "$symbols" \
  --start 2021-09-01 \
  --end 2022-12-31 \
  --eligibility-start 2022-01-01 \
  --eligibility-end 2022-12-31 \
  --readiness-json research/results/playbooks/classical_pattern_lab/semantic_round_1/readiness/data_readiness.json \
  --batch-id rectangle-semantic-calibration-v1-r1 \
  --batch-size 12 \
  --output-dir "$batch_root"

uv run --frozen python -m src.research.classical_patterns.runner verify-semantic-batch \
  --batch-dir "$batch_root"
```

The request is 12 cards, but the full eligible 2022 positive population is ten;
the generator reports the shortfall instead of silently importing another split.
Generation refuses a nonempty output directory, so every round receives a new
batch root rather than inheriting stale cards from an earlier attempt.

## Human Review

Open `REVIEW_INDEX.md`, inspect each as-of card, and fill these columns in
`review_responses.csv`:

- `decision`: `accept`, `revise`, `reject`, or `ambiguous`;
- four fidelity fields: `yes`, `no`, `revise`, or `ambiguous`;
- corrections and reason codes when needed;
- reviewer, review timestamp, and both hidden/future attestations as `true`.

Allowed reason codes are `not_rectangle`, `insufficient_touch_structure`,
`trend_not_balance`, `boundary_misplaced`, `breakout_not_confirmed`,
`direction_wrong`, `lfd_misidentified`, `pattern_morphed`,
`chart_data_suspect`, `insufficient_context`, and `other`; separate multiple
codes with commas. Accepted rows require all four fidelity fields to be `yes`,
and revisions must include a correction or reason code.

Ingestion is append-only and idempotent:

```bash
uv run --frozen python -m src.research.classical_patterns.runner ingest-semantic-responses \
  --batch-dir research/results/playbooks/classical_pattern_lab/semantic_round_1/review_batch_2022_v1
```

It rejects stale hashes, unknown cards/enums, conflicting repeat decisions, or
missing hidden/future attestations. The resulting semantic scorecard contains
no economic fields.

## Next Decision

- If the ten examples are mostly faithful, add deterministic negative/near-miss
  observations before measuring recall.
- If boundaries or Last Full Day are systematically wrong, revise the doctrine
  and config under version 2, then create a new untouched semantic round.
- Do not run an economic holdout until semantic rules and a licensed
  point-in-time data manifest are frozen.
