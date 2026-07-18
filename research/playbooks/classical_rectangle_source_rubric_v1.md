# Classical Rectangle Source-Fidelity Rubric v1

**Status:** frozen review rubric
**Rubric id:** `classical-rectangle-source-fidelity`
**Version:** `1`
**Date:** `2026-07-17`
**Scope:** outcome-hidden review of daily rectangle structure and completion

This rubric separates what the cited public sources actually support from the
deterministic choices Mala must make to test one narrow implementation. It does
not claim to reproduce Peter Brandt's private judgment, portfolio, or results.

## Sources

1. Peter Brandt, [The Four Key Pillars of Factor](https://www.peterlbrandt.com/knowledge-center/four-key-pillars-factor/)
2. Peter Brandt, [July Soybeans — A Chart Lesson](https://www.peterlbrandt.com/july-soybeans-a-chart-lesson/)
3. Aksel Kibar, [A Framework for Classifying Chart Pattern Breakouts](https://blog.techcharts.net/2026/07/14/a-framework-for-classifying-chart-pattern-breakouts/)

The TechCharts article explicitly extends Brandt's Last Full Day risk concept
into a Type 1–4 post-breakout taxonomy. The taxonomy is therefore an attributed
extension, not a claim that Brandt published those four labels.

## Source-Supported Principles

Reviewers may treat the following as source-grounded:

- classical chart patterns are used to seek asymmetric reward-to-risk setups;
- chart labeling is subjective, two chartists may disagree, and patterns can
  morph after an apparent completion;
- a completion/entry signal must be distinguishable from an unfinished pattern;
- closing-price confirmation can be a legitimate completion tactic;
- the Last Full Day is the last completed day whose full high-low bar remains
  inside the pattern before breakout;
- for a long, the initial LFD risk reference is below that day's low; for a
  short, it is above that day's high;
- an LFD violation weakens trade quality but is distinct from structural
  pattern negation;
- a measured objective and structural negation can be used to classify the
  subsequent breakout path;
- risk and trade management matter at least as much as pattern selection.

## Mala Operationalizations — Not Peter Claims

The following are predeclared hypotheses needed for deterministic testing.
They are not presented as universal Brandt doctrine:

- daily U.S. equity/ETF bars;
- horizontal rectangles only;
- 20, 40, and 60 completed-session candidate windows;
- centered pivot span of two sessions;
- minimum two touches per boundary and two alternations;
- three-session minimum touch separation;
- ATR-scaled boundary tolerance, breakout buffer, height bounds, and stop
  buffer variants;
- OLS close-drift and center-containment thresholds;
- next-session-open executable entry;
- one rectangle-height measured objective;
- zero re-entries in v1;
- fixed lifecycle and trade expiry;
- conservative stop-first ordering when daily bars cannot reveal intraday
  sequence.

Changing one of these choices creates a new tested definition or variant. A
better backtest cannot retroactively turn it into source doctrine.

## Outcome-Hidden Review Questions

Review only the bars visible through the stated cutoff. Do not consult later
bars, lifecycle labels, trades, P&L, or hidden detector classes.

### 1. Mala rectangle state

Choose:

- `no_mala_rectangle`: the frozen Mala v1 geometry is not credibly present;
- `mala_rectangle_no_close_breakout`: the frozen Mala v1 rectangle is present,
  but no fresh close-confirmed completion is visible at the cutoff;
- `mala_rectangle_long_close_breakout`: the frozen Mala v1 rectangle has a
  fresh upside close-confirmed completion at the cutoff;
- `mala_rectangle_short_close_breakout`: the frozen Mala v1 rectangle has a
  fresh downside close-confirmed completion at the cutoff;
- `indeterminate`: the visible evidence cannot support a reliable Mala-spec
  state.

Do not require a personally attractive trade. Judge the frozen rectangle idea.

### 2. Last Full Day assessment

Choose:

- `identified`: on a fresh completion, a last prior full bar inside the
  pattern can be identified;
- `not_applicable`: there is no fresh completion at the cutoff;
- `indeterminate`: structure or boundary ambiguity prevents a reliable call.

If identified, record `lfd_date` as `YYYY-MM-DD`; otherwise leave it blank.

### 3. Reason and provenance codes

Use zero or more `spec_reason_codes` when the Mala state is rejected or
indeterminate:

- `mala_not_horizontal_balance`
- `mala_insufficient_touch_structure`
- `mala_trend_not_balance`
- `mala_boundary_misplaced`
- `mala_range_height_out_of_bounds`
- `mala_close_containment_failure`
- `mala_touch_recency_failure`
- `mala_breakout_not_close_confirmed`
- `mala_breakout_direction_wrong`
- `techcharts_lfd_misidentified`
- `pattern_morphed_or_competing_label`
- `insufficient_visible_context`
- `chart_data_suspect`
- `other`

Use zero or more `source_ambiguity_codes` when the cited corpus is silent or
non-universal:

- `source_rectangle_geometry_undefined`
- `source_breakout_completion_nonuniversal`
- `source_lfd_exact_rule_secondary_only`
- `source_lfd_boundary_edge_case_undefined`
- `source_negation_level_undefined`
- `source_objective_formula_undefined`
- `source_meaningful_retest_undefined`
- `source_type_event_ordering_undefined`
- `source_reentry_rule_nonuniversal`

These labels evaluate a frozen Mala operationalization and its honest source
boundary. They cannot filter or weight the economic population. A semantic
freeze is named `MalaRectangleSemanticSpecV1`; it is not a declaration of a
publicly defined Peter Brandt rectangle method.

## Freeze Gate

The source-fidelity contract may freeze when:

- two independent outcome-blind review passes cover every card;
- per-card exact agreement on `mala_rectangle_state` and LFD assessment is
  measured and reported against the predeclared threshold;
- disagreements are retained rather than silently majority-voted;
- every hard detector threshold is labeled as source-supported or Mala-owned;
- no review response can enter signal eligibility or economic selection;
- unresolved source ambiguity is documented as a limitation or a new
  predeclared variant before outcomes are examined.
