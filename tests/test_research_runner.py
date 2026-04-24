from __future__ import annotations

from pathlib import Path

import pytest

from src.research.research_runner import create_hypothesis_file


def test_create_hypothesis_file_writes_standard_config(tmp_path: Path) -> None:
    path = create_hypothesis_file(
        hypothesis_id="My Test Idea",
        title="My Test Idea",
        strategy="Market Impulse (Cross & Reclaim)",
        symbol_scope="AMD",
        max_stage="M1",
        thesis="AMD may continue after opening impulse.",
        rules=["Use Market Impulse defaults."],
        notes=["Feasibility tag: config-only."],
        hypotheses_dir=tmp_path,
    )

    text = path.read_text(encoding="utf-8")
    assert path.name == "my-test-idea.md"
    assert "- id: `my-test-idea`" in text
    assert "- state: `pending`" in text
    assert "- symbol_scope: `AMD`" in text
    assert "- strategy: `Market Impulse (Cross & Reclaim)`" in text


def test_create_hypothesis_file_refuses_unknown_strategy(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        create_hypothesis_file(
            hypothesis_id="bad",
            title="Bad",
            strategy="Not A Strategy",
            symbol_scope="AMD",
            max_stage="M1",
            thesis="",
            rules=[],
            notes=[],
            hypotheses_dir=tmp_path,
        )
