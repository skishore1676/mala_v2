# mala_v2 — bootstrap contract (Layer 0)

You are inside the head of Suman's automated options-trading system. This repo (mala_v2)
does research, backtesting, and evidence gating; its strategies execute real money via
**bhiksha** on the always-on Mac "oldmac". A month-long live experiment of the operator's
own exit playbooks has been running since 2026-07-02. This repo is also the system's
**brain**: the durable memory any agent session inherits.

## Safety posture — read before acting

- **Live money is downstream of this repo.** The bhiksha checkout on oldmac
  (`/Users/sunny/Documents/bhiksha`) is the production runtime; its deploys are gated on
  green tests + adversarial audit + session boundary. Never deploy, write to the Google
  Sheet, place orders, or mutate oldmac without those gates and explicit authority.
- **Money-path changes require adversarial audit rounds** — a green test suite is not
  proof. The same-auditor re-run round has caught the worst bug four consecutive times.
- Default stance for new sessions: **read-and-recommend.** Execution authority is granted
  by the operator per lane, not inherited from this file.

## Reading protocol (beating amnesia)

1. Before ANY live-loop work, read the brain index: @docs/brain/INDEX.md
2. Deep knowledge on demand: `docs/brain/{ARCHITECTURE,OPERATIONS,DECISIONS,STATE}.md`
   (each carries `as_of` + `sources` frontmatter — a claim without a citation is not
   admissible; if `as_of` is stale, re-verify against primary sources).
3. Episodic record: `docs/LIVE_LOOP_WORKPLAN.md` (status board + dated diary — the
   canonical running document of the live experiment).
4. **Trust order: runtime evidence > diary > brain summary.** When they disagree, the
   runtime (oldmac readback, sqlite, broker payloads) wins.

## House rules

- This repo is **committed locally and deliberately NOT pushed** (P&L and strategy edge
  live here; remote is public). Do not push without the operator's say.
- Repo-local engineering lessons live in `docs/lessons/` here and in
  `bhiksha:docs/lessons/` — code-adjacent truth stays with its code; the brain indexes it.
- Personal operator facts (trading DNA, profile) live in memory_core / the Claude memory
  dir, not in this repo.
- `AGENTS.md` + `agent.md` carry the research-workbench skill map for research work.
