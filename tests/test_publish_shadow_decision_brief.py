from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "publish_shadow_decision_brief.py"


def test_shadow_brief_uses_vault_relative_report_links(tmp_path: Path) -> None:
    mala_root = tmp_path / "mala_v2"
    daily_dir = mala_root / "data" / "results" / "research_ops" / "shadow_campaign" / "daily"
    signal_dir = mala_root / "data" / "results" / "research_ops" / "bhiksha_signal_ev" / "run"
    vault = tmp_path / "northstar"
    daily_dir.mkdir(parents=True)
    signal_dir.mkdir(parents=True)

    daily_report = daily_dir / "shadow_daily_report_20260518T203713Z.md"
    daily_report.write_text("- runtime_issue_rows: `0`\n", encoding="utf-8")
    signal_report = signal_dir / "BHIKSHA_SIGNAL_EV_REPORT.md"
    signal_report.write_text(
        "\n".join(
            [
                "- true_signal_events: `2`",
                "- trade_plans: `2`",
                "- closed_trades_with_realized_ev: `1`",
                "- total_realized_pnl_usd: `135.0`",
                "- positive_trades_vs_evidence: `1`",
                "- adverse_trades_vs_evidence: `0`",
                "",
                "## Same-Bar Mala Replay",
                "| Status | Count |",
                "|---|---:|",
                "| match | 1 |",
                "| missing_bar | 1 |",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (signal_dir / "bhiksha_signal_ev_deployments.csv").write_text(
        "deployment_id,signal_count,closed_trade_count,total_realized_pnl_usd,avg_realized_stop_r,mala_expected_r_used,operator_verdict\n"
        "demo,2,1,135.0,0.45,0.73,small_sample_positive\n",
        encoding="utf-8",
    )
    (signal_dir / "bhiksha_signal_counterfactual.csv").write_text(
        "root_cause\nmala_cache_gap\n",
        encoding="utf-8",
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--mala-root",
            str(mala_root),
            "--vault-root",
            str(vault),
            "--output-dir",
            "03 Agent Org/research_lab/Mala/Shadow",
            "--trading-date",
            "2026-05-18",
            "--daily-report",
            str(daily_report),
            "--signal-ev-report",
            str(signal_report),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr + proc.stdout
    note = vault / "03 Agent Org" / "research_lab" / "Mala" / "Shadow" / "2026-05-18.md"
    text = note.read_text(encoding="utf-8")

    daily_link = (
        "[shadow_daily_report_20260518T203713Z.md]"
        "(<03 Agent Org/research_lab/Mala/Shadow/attachments/2026-05-18/shadow_daily_report_20260518T203713Z.md>)"
    )
    signal_link = (
        "[BHIKSHA_SIGNAL_EV_REPORT.md]"
        "(<03 Agent Org/research_lab/Mala/Shadow/attachments/2026-05-18/BHIKSHA_SIGNAL_EV_REPORT.md>)"
    )

    assert f"- daily_report: {daily_link}" in text
    assert f"- signal_ev_report: {signal_link}" in text
    assert f"- {daily_link} from `{daily_report}`" in text
    assert f"- {signal_link} from `{signal_report}`" in text
    assert f"`{vault}" not in text


def test_shadow_brief_relative_path_helper_handles_symlinked_vault(tmp_path: Path) -> None:
    real_vault = tmp_path / "vault-real"
    link_vault = tmp_path / "vault-link"
    target = real_vault / "03 Agent Org" / "research_lab" / "Mala" / "Shadow" / "attachments" / "2026-05-18" / "report.md"
    target.parent.mkdir(parents=True)
    target.write_text("ok\n", encoding="utf-8")
    link_vault.symlink_to(real_vault, target_is_directory=True)

    spec = importlib.util.spec_from_file_location("publish_shadow_decision_brief", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert (
        module._vault_relative(target, link_vault)
        == "03 Agent Org/research_lab/Mala/Shadow/attachments/2026-05-18/report.md"
    )
