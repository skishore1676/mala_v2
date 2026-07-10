# Bootstrap contract home = AGENTS.md (agent-agnostic); CLAUDE.md = thin @import

**Claim:** As of 2026-07-09 22:08, the agent bootstrap contract lives in `AGENTS.md`
(agent-agnostic) in both mala_v2 and bhiksha; `CLAUDE.md` is now a thin `@import` pointer
rather than the source of truth. Resolves RFC §9a Q6.

**Why:** Keeps the bootstrap contract portable across agents/tools instead of Claude-specific,
so any coding agent loads the same brain entry point. bhiksha carries only a thin CLAUDE.md
stub pointing at the mala_v2 brain (never the money path).

**Evidence:** mala_v2 `72e00fa` (07-09 22:08); bhiksha `0f66ad1` + `46a9d0d`
(dev-Mac checkout, doc-only, NOT yet on oldmac runtime). [mala_v2 git log; bhiksha git log]

**For curator:** likely belongs in ARCHITECTURE (bootstrap/entry-point topology) and/or a
DECISIONS ADR extending the brain-home decision (mala_v2, ADR-012 neighborhood).
