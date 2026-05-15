"""Create a deterministic operator policy card from a playbook query result."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.research.playbook_consultation_log import update_consultation_row
from src.research.playbook_operator_policy import OperatorPolicy, operator_policy_from_payload


@dataclass(frozen=True, slots=True)
class PolicyCardResult:
    out_dir: Path
    markdown_path: Path
    json_path: Path
    policy: str
    selected_exit: str


def build_policy_card(
    query_json: Path,
    *,
    out_dir: Path | None = None,
    update_log: bool = False,
    run_dir: Path | None = None,
    operator_policy_config: Path | None = None,
) -> PolicyCardResult:
    payload = json.loads(query_json.read_text(encoding="utf-8"))
    operator_policy = operator_policy_from_payload(
        payload,
        override_path=operator_policy_config,
    )
    card = _policy_card_payload(payload, query_json, operator_policy)
    destination = out_dir or query_json.parent
    destination.mkdir(parents=True, exist_ok=True)
    json_path = destination / "policy_card.json"
    markdown_path = destination / "POLICY_CARD.md"
    json_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_policy_card_markdown(card), encoding="utf-8")
    if update_log and card["policy"] == "take" and card["selected_exit"]:
        effective_run_dir = run_dir or _run_dir_from_payload(payload)
        if effective_run_dir is None:
            raise ValueError("--run-dir is required when source_run is absent from query_result.json")
        update_consultation_row(
            effective_run_dir,
            query_id=card["query_id"],
            selected_exit=card["selected_exit"],
            reported_survived_pct=card["exit"].get("survived_pct", ""),
            operator_note=f"policy_card:{card['policy']}:{card['rule_id']}",
        )
    return PolicyCardResult(
        out_dir=destination,
        markdown_path=markdown_path,
        json_path=json_path,
        policy=str(card["policy"]),
        selected_exit=str(card["selected_exit"]),
    )


def _policy_card_payload(
    payload: dict[str, Any],
    query_json: Path,
    operator_policy: OperatorPolicy,
) -> dict[str, Any]:
    cohort = payload.get("cohort", {})
    management_rows = list(cohort.get("management_rows", []))
    selected_exit = _select_exit(management_rows, operator_policy)
    policy, policy_reason = _policy_decision(payload, selected_exit, operator_policy)
    journal_selected_exit = selected_exit.get("exit_family", "") if policy == "take" else ""
    journal_survived = selected_exit.get("survived_pct", "") if policy == "take" else ""
    watch = _watch_lines(payload)
    return {
        "query_id": query_json.parent.name,
        "query_json": str(query_json),
        "playbook_id": str(payload.get("playbook_id", "")),
        "symbol": str(payload.get("symbol", "")),
        "direction": str(payload.get("direction", "")),
        "timestamp_et": str(payload.get("timestamp_et", "")),
        "read": {
            "desk_read": str(payload.get("verdict", "")),
            "confidence": str(cohort.get("confidence", "")),
            "cohort_n": int(_safe_float(cohort.get("analog_count")) or 0),
            "candidate_count": int(_safe_float(cohort.get("candidate_count")) or 0),
        },
        "policy": policy,
        "rule_id": operator_policy.rule_id,
        "operator_policy": operator_policy.to_payload(),
        "policy_reason": policy_reason,
        "best_exit": selected_exit.get("exit_family", ""),
        "selected_exit": journal_selected_exit,
        "exit": selected_exit,
        "stop": _stop_payload(selected_exit),
        "watch": watch,
        "journal_prefill": {
            "selected_exit": journal_selected_exit,
            "reported_survived_pct": journal_survived,
            "taken": "",
            "actual_exit_reason": "",
            "actual_pnl_r": "",
            "actual_time_to_exit": "",
        },
        "agent_caveat_contract": (
            "Optional external-context agents may add caveats, but must not silently "
            "override this deterministic policy."
        ),
    }


def _policy_decision(
    payload: dict[str, Any],
    selected_exit: dict[str, str],
    operator_policy: OperatorPolicy,
) -> tuple[str, str]:
    cohort = payload.get("cohort", {})
    verdict = str(payload.get("verdict", ""))
    confidence = str(cohort.get("confidence", ""))
    cohort_n = int(_safe_float(cohort.get("analog_count")) or 0)
    survived = _parse_pct(selected_exit.get("survived_pct"))
    entry_window = payload.get("entry_window", {})
    if entry_window.get("in_entry_window") == "no":
        return "out_of_scope", "query timestamp is outside the playbook entry window"
    if verdict not in operator_policy.take_verdicts:
        return "pass", (
            f"desk_read {verdict!r} is not in take set {sorted(operator_policy.take_verdicts)}"
        )
    if not operator_policy.confidence_at_least(confidence):
        return "pass", f"confidence {confidence!r} is below {operator_policy.min_confidence}"
    if cohort_n < operator_policy.min_cohort_n:
        return "pass", f"cohort {cohort_n} is below minimum {operator_policy.min_cohort_n}"
    if not selected_exit:
        return "pass", "no usable management row survived the tradability filters"
    if survived is None or survived < operator_policy.min_exit_survived:
        return "wait", (
            f"best exit survived {selected_exit.get('survived_pct', '')}, "
            f"below {operator_policy.min_exit_survived:.0%}"
        )
    return (
        "take",
        (
            f"matches {operator_policy.rule_id}: "
            f"desk_read in {sorted(operator_policy.take_verdicts)}, "
            f"conf >= {operator_policy.min_confidence}, "
            f"cohort >= {operator_policy.min_cohort_n}, "
            f"exit survived >= {operator_policy.min_exit_survived:.0%}"
        ),
    )


def _select_exit(
    rows: list[dict[str, str]],
    operator_policy: OperatorPolicy,
) -> dict[str, str]:
    usable = [row for row in rows if _parse_pct(row.get("survived_pct")) is not None]
    if not usable:
        return {}
    if operator_policy.exit_selection != "max_survived_then_target_move":
        raise ValueError(f"Unsupported exit_selection: {operator_policy.exit_selection}")
    return max(
        usable,
        key=lambda row: (
            _parse_pct(row.get("survived_pct")) or -1.0,
            _safe_float(row.get("median_target_move")) or -1.0,
        ),
    )


def _stop_payload(exit_row: dict[str, str]) -> dict[str, str]:
    if not exit_row:
        return {}
    return {
        "stop_reference": exit_row.get("stop_reference", ""),
        "stop_move": exit_row.get("median_stop_move", ""),
        "reward_risk": exit_row.get("reward_risk", ""),
    }


def _watch_lines(payload: dict[str, Any]) -> list[str]:
    summary = payload.get("cohort", {}).get("outcome_summary", {})
    lines: list[str] = []
    fifteen = _parse_pct(summary.get("15", {}).get("reversion_pct"))
    sixty = _parse_pct(summary.get("60", {}).get("reversion_pct"))
    eod = _parse_pct(summary.get("eod", {}).get("reversion_pct"))
    if fifteen is not None and sixty is not None and fifteen - sixty >= 0.10:
        lines.append(
            f"60m reversion erodes from {fifteen:.0%} to {sixty:.0%}; this is a fast trade."
        )
    if fifteen is not None and eod is not None:
        if eod < 0.50:
            lines.append(
                f"EOD reversion erodes to {eod:.0%}; this is a fast trade, not a hold."
            )
        elif fifteen - eod >= 0.10:
            lines.append(f"EOD reversion decays from {fifteen:.0%} to {eod:.0%}.")
    if not lines:
        lines.append("No horizon-decay warning from the 15m/60m/EOD cohort summary.")
    return lines


def _policy_card_markdown(card: dict[str, Any]) -> str:
    read = card["read"]
    exit_row = card.get("exit", {})
    stop = card.get("stop", {})
    candidate_count = f"{read['candidate_count']:,}" if read.get("candidate_count") else "?"
    exit_line = "none"
    if exit_row:
        exit_line = (
            f"{exit_row.get('exit_family', '')}  |  survived {exit_row.get('survived_pct', '')}  |  "
            f"target {exit_row.get('median_target_move', '')} pts  |  median "
            f"{exit_row.get('median_time_to_target_min', '')} min"
        )
    stop_line = "none"
    if stop:
        stop_line = (
            f"{stop.get('stop_reference', '')} {stop.get('stop_move', '')} pts "
            f"({stop.get('reward_risk', '')}:1)"
        )
    lines = [
        "# Playbook Policy Card",
        "",
        (
            f"READ:    {read.get('desk_read', '')}  |  {read.get('confidence', '')} confidence  |  "
            f"cohort {read.get('cohort_n', '')}/{candidate_count}"
        ),
        f"POLICY:  {card.get('policy', '')}  ({card.get('policy_reason', '')})",
        f"EXIT:    {exit_line}",
        f"STOP:    {stop_line}",
        "WATCH:   " + " ".join(card.get("watch", [])),
        "",
        "## Journal Prefill",
        "",
        f"- query_id: `{card.get('query_id', '')}`",
        f"- selected_exit: `{card.get('journal_prefill', {}).get('selected_exit', '')}`",
        f"- reported_survived_pct: `{card.get('journal_prefill', {}).get('reported_survived_pct', '')}`",
        "",
        "## Operator Policy",
        "",
        f"- policy_id: `{card.get('operator_policy', {}).get('policy_id', '')}`",
        f"- policy_version: `{card.get('operator_policy', {}).get('policy_version', '')}`",
        f"- source_path: `{card.get('operator_policy', {}).get('source_path', '')}`",
        f"- rule_id: `{card.get('rule_id', '')}`",
        "",
        "## Agent Caveat Contract",
        "",
        card.get("agent_caveat_contract", ""),
        "",
    ]
    return "\n".join(lines)


def _run_dir_from_payload(payload: dict[str, Any]) -> Path | None:
    source_run = str(payload.get("source_run", ""))
    return Path(source_run) if source_run else None


def _safe_float(raw: Any) -> float | None:
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value


def _parse_pct(raw: Any) -> float | None:
    if raw in (None, ""):
        return None
    value = str(raw).strip()
    if value.endswith("%"):
        value = value[:-1]
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return parsed / 100.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-json", required=True, type=Path)
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--update-log", action="store_true")
    parser.add_argument("--run-dir", type=Path, default=None)
    parser.add_argument(
        "--operator-policy-config",
        type=Path,
        default=None,
        help="Optional YAML policy file. Defaults to the policy embedded in query_result.json.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = build_policy_card(
        args.query_json,
        out_dir=args.out_dir,
        update_log=args.update_log,
        run_dir=args.run_dir,
        operator_policy_config=args.operator_policy_config,
    )
    print(f"POLICY_CARD={result.markdown_path}")
    print(f"POLICY_JSON={result.json_path}")
    print(f"POLICY={result.policy}")
    if result.selected_exit:
        print(f"SELECTED_EXIT={result.selected_exit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
