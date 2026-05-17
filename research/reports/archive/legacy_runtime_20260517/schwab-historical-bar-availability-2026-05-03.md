# Schwab Historical Bar Availability - 2026-05-03

## Question

If Schwab has enough historical minute bars, Mala could rebuild strategies on
the same provider Bhiksha uses at runtime and avoid Polygon-vs-Schwab volume
divergence.

## Local Runtime Probe

Probe was run read-only on oldmac from:

```text
~/Documents/bhiksha
```

Endpoint:

```text
https://api.schwabapi.com/marketdata/v1/pricehistory
```

Symbol:

```text
AMD
```

Important parameters:

```text
frequencyType=minute
needExtendedHoursData=false
```

## Results

| Request | Candles | First Bar UTC | Last Bar UTC | Read |
|---|---:|---|---|---|
| 1m, 365 calendar days, regular hours | 12870 | 2026-03-17 13:30 | 2026-05-01 19:59 | rolling recent window only |
| 1m, 730 calendar days, regular hours | 12870 | 2026-03-17 13:30 | 2026-05-01 19:59 | same rolling window |
| 1m, endDate 2026-03-16 | 0 | | | cannot page earlier |
| 1m, endDate 2026-02-02 | 0 | | | cannot page earlier |
| 5m, 365 calendar days, regular hours | 13881 | 2025-08-18 13:30 | 2026-05-01 19:55 | about 8-9 months |
| 5m, endDate 2026-03-16 | 4755 | 2025-12-16 | 2026-03-16 | can page within available 5m range |
| 15m, endDate 2026-03-16 | 1586 | 2025-12-16 | 2026-03-16 | can page within available 15m range |
| 30m, endDate 2026-03-16 | 793 | 2025-12-16 | 2026-03-16 | can page within available 30m range |
| Daily, 365 calendar days | 250 | 2025-05-05 | 2026-05-01 | daily history is available |

## Interpretation

Observed:
Schwab 1-minute equity bars are available, but only for a recent rolling window.
On 2026-05-03, AMD regular-hours 1-minute history started at 2026-03-17.

Observed:
Changing the requested start date to 365 or 730 calendar days did not extend the
1-minute result. Setting older `endDate` values returned empty 1-minute results.

Inferred:
Schwab cannot replace Polygon as Mala's long-horizon 1-minute historical source
for the current M1-M5 backtest workflow.

Observed:
Schwab 5/15/30-minute bars are available farther back and can page across older
end dates within the available retention period.

Inferred:
Schwab could support a shorter or coarser research track, such as a 5-minute
runtime-provider validation layer, but not a full rebuild of existing 1-minute
Mala evidence.

## Recommendation

Do not migrate Mala's canonical historical 1-minute backtests from Polygon to
Schwab.

Use Schwab historical bars for:

- runtime-provider validation over the recent 1-minute window;
- 5-minute or coarser provider-native sanity checks;
- post-shadow replay around actual Bhiksha events.

Keep Polygon for:

- long-horizon 1-minute M1-M5 research;
- holdout and Monte Carlo evidence requiring multi-month or multi-year samples.

If we want exact provider parity for live trading, the practical options remain:

- make volume logic provider-invariant;
- use Polygon live for shadow/live signal bars;
- maintain a rolling Schwab 1-minute archive from today forward, accepting that
  older Mala evidence remains Polygon-based until enough Schwab history accrues.
