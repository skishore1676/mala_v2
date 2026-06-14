"""Import helper for the Mala/Bhiksha shared kernel during the refactor."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_kernel_on_path() -> None:
    try:
        import mala_bhiksha_kernel  # noqa: F401

        return
    except ModuleNotFoundError:
        pass

    # Look for a sibling ``mala-bhiksha-kernel/src`` checkout. The first
    # candidate is the historical layout (kernel beside the repo root). The
    # remaining candidates walk up the ancestors so the helper still resolves
    # when Mala is checked out in a git worktree (e.g. ``.../mala_v2/.claude/
    # worktrees/<id>``), where the kernel lives beside the *real* repo root
    # rather than beside the worktree.
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [repo_root.parent / "mala-bhiksha-kernel" / "src"]
    candidates += [
        ancestor / "mala-bhiksha-kernel" / "src" for ancestor in repo_root.parents
    ]
    for sibling_src in candidates:
        if sibling_src.exists():
            sys.path.insert(0, str(sibling_src))
            return
