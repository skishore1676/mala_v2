# OpenClaw Research Ops Handoff - Mala Shadow Decision Week

## Operating Role

Research Ops owns the daily evidence loop for the Mala/Bhiksha shadow campaign.
Suman should not need to inspect raw logs unless the daily decision brief cannot
assign ownership.

## Canonical Protocol

Read:

- `/Users/sunny/Documents/mala_v2/docs/SHADOW_DECISION_PROTOCOL_NEXT_WEEK.md`
- latest Obsidian brief under
  `/Users/sunny/Library/Mobile Documents/iCloud~md~obsidian/Documents/northstar/areas/trading/mala-shadow/`

## Daily Automation

Cron calls:

```bash
/Users/sunny/.openclaw/workspace/scripts/mala-shadow-daily.sh
```

That wrapper runs:

1. Bhiksha active-plan sync.
2. Polygon cache backfill for active-plan symbols.
3. Bhiksha bionic session review.
4. Mala shadow daily report.
5. Mala/Bhiksha signal EV and counterfactual report.
6. Obsidian decision brief publication.

## Daily Research Ops Readout

When asked "how are we doing", answer from the Obsidian decision brief first.
Use raw artifacts only to support the brief.

Classify the day into one owner:

- `Bhiksha plumbing`: runtime lifecycle, missing evals, shadow accounting, exit
  recording, active-plan compile issues.
- `Provider/data contract`: provider_feature_mismatch, volume/VPOC/directional
  mass divergence, live feature source mismatch.
- `Broker/execution`: option quote/fill/lifecycle evidence after signal parity
  is clean.
- `Strategy/exit`: clean matched signals lose money or exits do not match the
  thesis.
- `More sample`: no decision-sized clean matched sample yet.

## Escalation Rules

Escalate to Codex when:

- the Obsidian brief owner is Bhiksha plumbing or provider/data contract;
- the report cannot answer a gate due to missing fields;
- same-bar match falls below 95%;
- provider_feature_mismatch repeats on traded deployments;
- clean matched trades reach 20 and average realized option R is negative.

Do not recommend live promotion unless all gates in
`SHADOW_DECISION_PROTOCOL_NEXT_WEEK.md` are met.
