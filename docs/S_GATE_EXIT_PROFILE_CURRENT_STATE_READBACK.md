# S-Gate / Exit-Profile Current State Readback

Status: readback for oldmac deployment planning, 2026-06-14.

This memo is the shared checkpoint before any oldmac deployment. It does not
change strategy authorization, Google Sheets, active plans, broker state, or
runtime flags.

## Objective

Get current on what exists across:

- `mala_v2` research and publication tooling;
- `mala-bhiksha-kernel` shared contract;
- `bhiksha` active-plan compilation and runtime shadow handling.

The immediate question is not "is profile exit live?" It is:

> Has the source-level chain advanced enough that we can safely sync it to
> oldmac, prove it there, and then decide whether any runtime deployment step is
> warranted?

## Source Heads Checked

Local source state checked on 2026-06-14:

| Repo | Local branch/head | Current role |
| --- | --- | --- |
| `mala_v2` | `8a0af42` `Merge exit-profile Sheet publisher (dry-run default, --commit explicit, oldmac-run)` | Exit-profile classify/explore/propose and dry-run Sheet publisher |
| `mala-bhiksha-kernel` | `552d029` `Merge: v2 ManagementPolicySpec exit-profile fields (capability v2)` | Shared v2 management-policy contract |
| `bhiksha` | `86abf11` `Merge P4 end-to-end shadow chain receipt driver (proof tool)` | Active-plan bridge plus shadow receipt proof |

Local dirty/untracked items observed and intentionally preserved:

- `mala_v2`: `.claude/`, `.super-goal/`, `.supervisor-lane/`
- `mala-bhiksha-kernel`: `.DS_Store`
- `bhiksha`: `research/results/exit_profile_e2e_shadow/20260614T174142Z/`

## Oldmac Drift Checked

Read-only oldmac check showed the runtime machine is behind local/source main in
all three repos:

| Oldmac path | Oldmac head observed | Drift |
| --- | --- | --- |
| `/Users/sunny/Documents/mala_v2` | `db1e6f7` `Gate underlying profile candidates (off by default) + machine-aware kamandal db path` | Behind exit-profile publisher/classify-propose source |
| `/Users/sunny/Documents/mala-bhiksha-kernel` | `81b2d4d` `Add provider translation kernel contract` | Behind v2 `ManagementPolicySpec` capability source |
| `/Users/sunny/Documents/bhiksha` | `ad614ef` `Harden lane visibility, dead-lane alerting, and deploy integrity` | Behind profile-exit bridge/shadow receipt source |

Conclusion: any runtime statement today is source/local only until we sync and
read back oldmac. Do not restart oldmac services against the current oldmac
checkout and assume the exit-profile work is present.

## Current Positioning

### Mala

The canonical docs now say the active direction is an exit-profile lane with a
two-wave build:

- Wave 1: exits first on existing validated/live strategies.
- Wave 2: S0-S5 entry discovery for playbooks later.

Implemented source surfaces include:

- four named profiles in `src/research/exit_profiles.py`:
  `FLASH_REVERSAL`, `EXHAUSTION_REVERSAL`, `TREND_CONTINUATION`,
  `RANGE_EXPANSION`;
- first-pass strategy-to-profile assignments in `PROFILE_BY_STRATEGY`;
- option translation scoring in `src/research/option_translation.py`;
- profile-vs-legacy selection and kernel spec serialization in
  `src/research/exit_proposal.py`;
- classify/explore/propose orchestration in
  `scripts/classify_explore_propose.py`;
- dry-run-first Sheet publisher in `scripts/publish_exit_profiles.py`.

Important safety boundary: Mala proposes a frozen profile spec. It does not
turn profile exits live, mutate `active_strategy`, or sync oldmac by itself.

### Kernel

`mala-bhiksha-kernel` now has `ManagementPolicySpec` v2 fields for Tier-1
profile exits:

- staged targets: `target_1_r`, `target_2_r`, `target_1_quantity`;
- premium stops: `initial_stop_pct`, `premium_disaster_stop_pct`;
- time bounds: `no_progress_seconds`, `max_hold_seconds`;
- giveback and close behavior: `high_water_giveback_policy`,
  `breakeven_after_t1`, `eod_flat`.

These fields are additive and back-compatible. Vehicle selection such as DTE or
delta and sizing such as max capital are explicitly outside this contract.

### Bhiksha

Bhiksha now has the bridge and shadow proof surfaces:

- active-plan compiler reads a `management_policy_spec` / `exit_profile_spec`
  cell and maps kernel fields into deployment `ExitSpec`;
- `profile_exit_id` is derived from kernel `policy_id`;
- the bridge intentionally does not map or enable `profile_exit_drives_live`;
- runtime defaults remain `profile_exit_shadow_only=True` and
  `profile_exit_drives_live=False`;
- profile-exit evaluation records shadow events and only dispatches through the
  fail-closed allowlist when explicitly armed.

The local fixture receipt at
`bhiksha/research/results/exit_profile_e2e_shadow/20260614T174142Z/` proves a
real Mala Phase-3 proposal can travel through:

```text
kernel ManagementPolicySpec
-> Sheet-like management_policy_spec cell
-> compile_active_plan_from_sheet
-> ExitSpec
-> profile-exit shadow evaluator
```

The receipt stayed closed: dispatched `0`, orders `0`, broker untouched, no
Sheet write.

## S0-S5 Mapping Today

| Gate | Current state | Gap before claiming complete |
| --- | --- | --- |
| S0 Strategy Spec Lock | Documented in the S-gate spec and workbook-first doctrine. Profile names and first-pass strategy mappings exist in source. | Need a canonical per-candidate S0 receipt that records locked thesis, allowed surface, forbidden degrees of freedom, and option intent. |
| S1 Design Surface | Exit profile vs legacy comparison exists, using option-path expectancy with a profile-favoring margin. | Entry-discovery S1 for new playbook strategies is still Wave 2. Current Wave 1 is exit-first on existing strategies. |
| S2 OOS + Regime Readout | Classify/explore/propose uses an explicit holdout window for promoted rows. | Regime readout is not yet a unified published receipt across all profile candidates. |
| S3 Timing Robustness | Runtime shadow paths test exit-rule sequencing and state behavior. | Entry timing perturbations, one-minute bar trigger delay, missed fill, and spread/slip robustness are not yet first-class Mala gate artifacts. |
| S4 Option Translation + Exit Economics | Strongest implemented area: Black-Scholes option path scoring, IV bands, profile economics, v2 management spec serialization. | Needs oldmac readback and, later, real option-chain/provider calibration artifacts before stronger live claims. |
| S5 Provider / Broker Parity | Bhiksha compiler and shadow receipt prove the config bridge stays fail-closed. | Not yet proven on oldmac runtime against live provider/broker-observable state. Packet promotion, restart recovery, and feedback artifacts remain open. |

## Gaps I See

1. Oldmac is behind source main across all three repos.
2. There is not yet one canonical S0-S5 receipt per candidate tying the gate
   status together from Mala output through Bhiksha shadow proof.
3. S3 is still thin for entry timing and option fill realism. The exit shadow
   proof is useful, but it is not the same as entry timing robustness.
4. S5 is only fixture-shadow proof locally. It has not yet been proven by
   oldmac readback after source sync.
5. Vehicle selection and sizing are intentionally outside
   `ManagementPolicySpec`; they need separate contracts before full runtime
   ownership.
6. The live switch remains correctly manual. Mala/Sheet must not set
   `profile_exit_drives_live`; deployment should keep profile exits shadow-only
   until an explicit operator gate.
7. `scripts/publish_exit_profiles.py --commit` is a Google Sheet mutation and
   should remain approval-gated.

## Oldmac Deployment Gate

Recommended order before any service restart:

1. Preserve current oldmac dirty state in each repo.
2. Sync oldmac repos in dependency order:
   `mala-bhiksha-kernel` first, then `mala_v2`, then `bhiksha`.
3. On oldmac, run kernel contract tests, especially `tests/test_contracts.py`.
4. On oldmac, run Mala exit-profile tests:
   `tests/test_exit_profiles.py`, `tests/test_exit_proposal.py`,
   `tests/test_classify_explore_propose_entry.py`,
   `tests/test_exit_profile_publisher.py`, `tests/test_option_translation.py`.
5. On oldmac, run Bhiksha compiler/profile-exit tests covering active-plan
   mapping, shadow receipt, and armed-route fail-closed behavior.
6. Regenerate the Bhiksha profile-exit shadow receipt on oldmac using the
   oldmac checkouts and confirm again: dispatched `0`, orders `0`, broker
   untouched, no Sheet write.
7. Read back active-plan/runtime health without changing authorization.
8. Only after those receipts should we decide whether to restart any oldmac
   service. Restart is not the same as live enablement.

## Recommendation

Proceed to oldmac source sync and readback, but keep the runtime posture closed:

- `profile_exit_shadow_only=True`
- `profile_exit_drives_live=False`
- no Sheet commit unless explicitly approved
- no active-strategy mutation
- no broker/order path mutation

In plain terms: the source chain is worth deploying to oldmac for shadow proof.
It is not yet a reason to arm profile exits live.
