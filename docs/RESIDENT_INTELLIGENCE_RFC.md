# RFC: Resident Intelligence — a repo that learns

**Status:** DRAFT for operator review · 2026-07-09
**Author:** Claude (supervisor lane, live-loop week)
**Decision needed from:** Suman (see Open Questions, §9)

---

## 1. The vision, restated

You asked for "an agent like Claude, who continues to learn about this trading app, its
architecture, its ways of working... housed inside this repository itself" — so that the
repository "slowly becomes intelligent over time," built from substrates you already own
(Lathi bus, memory_core, agent broker), instead of every session starting as an agent
with amnesia.

The precise form this can take today: **the intelligence is not a resident process — it is
a resident memory plus a bootstrap contract.** Any visiting LLM (Claude Code, Codex, an
OpenClaw agent) wakes up inside the repo, reads a small always-loaded layer, and within a
minute *is* the agent that has been living here for weeks. The repo doesn't think between
sessions; it remembers, and it makes any thinker instantly resume. Learning loops keep the
memory current; your existing phone gate keeps it true.

This is not speculative. **The embryo already exists and worked for a week:**
`docs/LIVE_LOOP_WORKPLAN.md` (status board + dated diary), `.supervisor-lane/STATE.md`,
`docs/lessons/` here and in bhiksha, and the Claude session-memory at
`~/.claude/projects/-Users-suman-code-mala-v2/memory/` (7 curated notes + an index that
auto-loads). That layer is why the 07-09 session could run "fold all the build today" —
it woke up knowing the rails history, the audit discipline, and the oldmac quirks. The
RFC's job is to make that accidental embryo deliberate, durable, and substrate-backed.

## 2. Recommendation in brief

**Build `docs/brain/` inside mala_v2 as the canonical, git-versioned knowledge layer, with
a new root `CLAUDE.md`/`AGENTS.md` bootstrap contract that auto-loads a small curated
index.** File-truth in git is canonical; memory_core is a *projection* for out-of-repo
agents, never the source. Learning loops: (a) the existing compound/diary habits, made
mandatory by contract; (b) a nightly "brain steward" reflection job (agent-broker-run,
launchd-clocked on oldmac, which already has a mala_v2 checkout) that drafts updates as
*candidates*; (c) your Lathi-bus approve/revise card as the only path from candidate to
canon. The layer is read-and-recommend by default — it never touches money paths; lessons
about money-path code enter canon only carrying verification evidence, per this week's
adversarial-audit precedent. First step is one evening: scaffold the brain, promote the
existing embryo into it, and pass the fresh-session quiz.

---

## 3. What "learning" concretely looks like here (the embryo, surveyed)

| Artifact | What it holds | Weakness today |
|---|---|---|
| `mala_v2/docs/LIVE_LOOP_WORKPLAN.md` | Status board (25 items w/ deploy state) + dated diary — the system's episodic memory | One giant file; findable only if you know it exists |
| `mala_v2/.supervisor-lane/STATE.md` | How multi-worker builds are actually run (gates, audit verdicts, worktree lessons) | Lane-scoped; archived when lane closes |
| `mala_v2/docs/lessons/` (2), `bhiksha/docs/lessons/` (3) | Distilled, ref-cited patterns (shadow-first, audit rounds, Sheet-as-control-surface) | Nothing loads them at session start |
| `~/.claude/projects/-Users-suman-code-mala-v2/memory/` | 7 notes + MEMORY.md index, **auto-injected** into every Claude session here | Claude-only, machine-local, invisible to git/review, mixes personal + system facts |
| `mala_v2/docs/VEHICLE_POLICY_DECISION.md`, `EXIT_PROFILE_PLAYBOOKS.md`, etc. | Decision records with data provenance | No index; staleness invisible |

The bootstrap gap is concrete: **mala_v2 has no `CLAUDE.md`** (its `AGENTS.md` points at
`agent.md`, which onboards the *research workbench*, not the live loop); **bhiksha has
neither** — a fresh session in bhiksha knows nothing unless it stumbles on `docs/agent.md`.
The only thing beating amnesia today is the Claude-proprietary memory dir. That is the
single point of failure this design removes.

## 4. Substrate survey (what I actually read, and fit vs. the vision)

**Lathi bus** — `~/.claude/skills/lathi-review-bus/SKILL.md`; repo at
`/Users/suman/code/lathi-bus` (runtime also on oldmac). Publishes a file to the Obsidian
Inbox (`07 Agents/Coding`, profile `coding-agent-northstar`), you review on the phone with
`<pointy-bracket>` comments + approve/revise/park, `collect` returns a machine-readable
packet, frontmatter carries the lifecycle. **Proven this week**: P0 questionnaire
collected 07-04; session reports → approve/archive Inbox deployed 07-09 (workplan #6).
Fit: this is a ready-made human-curation gate for knowledge. No contradiction.

**memory_core (mc)** — `/Users/suman/code/memory_core` (README, `skills/memory/SKILL.md`);
daemon live on oldmac:8848 (verified responding); CLI `mc recall|propose|capabilities`.
Design: episodic `capture` is gate-free; durable memory exists **only after a human
approves a proposed candidate** (openclaw principal verified `can_write=true,
can_decide=false`; gate = the "01 Memory Review" card per `openclaw-ops/SKILL.md`).
Fit: philosophically identical to what we need (propose → human gate → canon) — but as a
*store* for repo knowledge it contradicts the vision: knowledge would live in a SQLite DB
on oldmac, unreviewable as diffs, invisible to `grep`/git, dependent on a daemon, and
outside the repo you said should become intelligent. Right role: **projection layer** so
out-of-repo agents (Jarvis on your phone) can recall trading state.

**agent broker** — `/Users/suman/code/agent-broker` (also on oldmac `~/code/agent-broker`).
Hires an agent by *role* with a provider-failover policy (claude → codex → openrouter),
returns an `AgentRunReceipt`; README documents exactly the headless-auth pattern
(CLAUDE_CODE_OAUTH_TOKEN via launchd env) a scheduled reflection job needs, and a
fail-closed `OUTCOME:` contract. Fit: the execution seam for automated learning loops —
found, real, no contradiction.

**Lathi / Control Tower** — `bhiksha/docs/lathi_control_tower_bhiksha_jobs.md`: "Bhiksha
is the engine. launchd is the clock. Lathi Control Tower is the cockpit." Apps own their
jobs; Lathi observes/projects. Fit: the brain steward follows the same contract — mala_v2
owns its brain job; nothing moves into Lathi.

**Claude Code bootstrap mechanics** (the crux): root `CLAUDE.md` auto-loads every session;
`CLAUDE.md` supports `@path` imports; `AGENTS.md` serves non-Claude agents; the memory dir
auto-injects `MEMORY.md`. The OpenClaw gateway offers a useful precedent for token
economics: allowlisted skills inject only name+description always-on, full body loads on
demand (`openclaw-ops/SKILL.md`) — the brain uses the same index-vs-body split.

## 5. The design

### 5.1 Where the intelligence lives — decision

**In mala_v2, at `docs/brain/`, as git-versioned files. mala_v2 is the head repo of the
4-repo organism** (the workplan, supervisor lane, evidence gates, and diary already live
here; oldmac already carries a checkout — `git remote -v` shows `oldmac:~/Documents/mala_v2`).
The brain has an explicit cross-repo charter: it *indexes* bhiksha/kernel/public_api_v3
knowledge and links to their `docs/lessons/`, but repo-local lessons stay in their repos
(preserving the `compound` skill's convention). bhiksha gets a 15-line `CLAUDE.md` stub
pointing at its own `docs/agent.md`, its lessons, and the mala_v2 brain.

File-truth vs database-truth: **git is canonical for everything about the system**
(architecture, decisions, lessons, current state). Reasons: (1) knowledge changes become
reviewable diffs — the same discipline as code; (2) it survives any substrate or vendor;
(3) the embryo is already file-shaped; (4) your phone gate (bus) reviews markdown
natively. **memory_core is canonical only for operator-personal facts** (your trading
DNA, preferences) and serves as a *derived projection* of the brain for out-of-repo
agents. The Claude memory dir shrinks to personal facts + pointers into the brain.

Rejected alternatives: **(a) memory_core as canonical store** — see §4; wrong medium for
reviewable system knowledge. **(b) A fifth "brain" repo** — adds sprawl, breaks
repo-local lessons, and mala_v2 is already the head. **(c) The Northstar vault/wiki** —
that is your reading surface; the bus projects *into* it; agents work in repos.
**(d) An always-on OpenClaw agent now** — operational surface + cost before there is a
substrate for it to inhabit; premature (see §8).

### 5.2 Brain layout

```
mala_v2/
  CLAUDE.md                     # NEW ~60 lines: identity, safety posture, @docs/brain/INDEX.md
  AGENTS.md                     # updated: same entry point for non-Claude agents
  docs/brain/
    INDEX.md                    # ≤150 lines. The map: one line per knowledge asset + as_of stamp
    ARCHITECTURE.md             # 4-repo organism, contracts, deploy topology, oldmac layout
    OPERATIONS.md               # runbook truth: launchd jobs, cadence, ssh/sqlite/bus idioms
    DECISIONS.md                # ADR ledger: exit-authority rule, rails 7.5/11.25, vehicle policy…
    STATE.md                    # "what is true right now" — rolling, dated, replaced not appended
    candidates/                 # drafts awaiting the bus gate; never loaded at bootstrap
```

The diary stays in `LIVE_LOOP_WORKPLAN.md` (episodic record, append-only); the brain
*distills* it. Lessons stay in `docs/lessons/`. Every brain file carries `as_of:` +
`sources:` frontmatter — a brain claim without a citation to code/diary/db is not
admissible.

### 5.3 The bootstrap contract (beating amnesia)

- **Layer 0 — always loaded (~1–2k tokens):** root `CLAUDE.md`: what this system is (three
  sentences), the safety posture (live money; read-and-recommend; gates), and the reading
  protocol: "before any live-loop work, read `docs/brain/INDEX.md`; trust order =
  runtime evidence > diary > brain summary."
- **Layer 1 — loaded via `@docs/brain/INDEX.md` import (~2–3k tokens):** the curated map —
  every asset one line with its `as_of` date, plus the 10 facts that must never be
  re-derived (oldmac runtime path, exit-authority rule, rails values, audit rule for
  money paths…).
- **Layer 2 — on demand (unbounded):** brain bodies, lessons, diary, evidence CSVs, live
  db readback. Retrieval is `grep`/`Read` — at this corpus size, deliberately no vector
  store (revisit in phase 3 only if grep demonstrably fails).

**Staleness handling:** `as_of` frontmatter everywhere; INDEX renders a freshness table;
the steward's nightly pass flags any brain file whose `as_of` predates commits touching
its `sources` (a ~50-line lint script). A stale file says so at the top — the contract
tells the agent to re-verify against the runtime rather than trust it. This mirrors the
lesson in `bhiksha/docs/lessons/armed-config-needs-gate-input-telemetry.md`: merged ≠
deployed; written ≠ still true — staleness must be a *visible, queryable* condition.

### 5.4 The learning loops (what writes, when, and how truth is curated)

| Loop | Trigger | Writes | Gate |
|---|---|---|---|
| L1 Diary | every watch/close (existing cadence) | dated entry in workplan diary | none — episodic, like mc `capture` |
| L2 Compound | session/lane end (existing `compound` skill) | `docs/lessons/<slug>.md` in the touched repo | code review of the PR/commit |
| L3 Brain steward | nightly (launchd, oldmac) via agent-broker | drafts to `docs/brain/candidates/` + freshness lint | **Lathi-bus card** approve/revise/park |
| L4 Architecture watch | weekly (same steward, Sunday) | candidate diffs to ARCHITECTURE/OPERATIONS after scanning the week's merges | same bus card |
| L5 Operator-triggered | "capture this / that's wrong" | candidate or direct edit | you are the gate |

The steward (L3) is a bounded, read-only reflection job: read today's diary entry + `git
log` across the repos + latest session report; draft a replacement `STATE.md` and any
DECISIONS/INDEX deltas; publish ONE bus card ("Brain update — YYYY-MM-DD") showing the
diff; on your approve, a small apply step commits candidate → canon; on revise, your
pointy-bracket comments drive round 2; on park/silence, canon is untouched. **Nothing
enters the always-loaded layer without passing your thumb.** This deliberately re-implements
memory_core's governance (capture free / propose gated / human decides) on the file
plane — same philosophy, reviewable medium.

Cadence realism: expect ~4 cards/week during active development, near-zero in quiet
weeks (the steward publishes nothing when the diff is empty). If that's too chatty, the
fallback is auto-committing `STATE.md` (episodic tier) and gating only
DECISIONS/ARCHITECTURE/INDEX — your call (§9 Q1).

### 5.5 Substrate mapping and the thin new glue

| Responsibility | Substrate | Status |
|---|---|---|
| Canonical memory + versioning + review diffs | **git** (mala_v2 `docs/brain/`) | exists |
| Session bootstrap injection | **CLAUDE.md/AGENTS.md + @import** | mechanism exists; files NEW |
| Curation gate + phone review | **Lathi bus** (`coding-agent-northstar`) | exists, proven 07-04/07-09 |
| Clock for reflection jobs | **launchd on oldmac** (app-owns-jobs; oldmac mala_v2 checkout) | pattern exists (bhiksha runs 7 jobs) |
| Headless LLM execution + failover + receipts | **agent broker** (`brain-steward` actor, policy claude→codex) | exists; needs a policy file |
| Cross-agent projection (Jarvis answers trading questions) | **memory_core** (`mc capture` a daily digest into a trading namespace) | exists; phase 3 wiring |
| Personal operator facts | **memory dir + memory_core** | exists; slim down to personal |

Genuinely new (all thin): `CLAUDE.md` ×2 + `docs/brain/` content (an evening, by hand);
`scripts/brain/steward_prompt.md` + broker policy + launchd plist (~an evening);
freshness lint (~50 lines); a `brain-apply` helper that moves an approved candidate into
canon and commits (~50 lines). **No new service, no new repo, no database.**

### 5.6 Safety and governance

1. **Read-and-recommend, structurally.** The brain and its steward have no execution
   authority: no Sheet writes, no deploys, no order-path anything. The steward's oldmac
   job runs with read-only db access (the `immutable=1` sqlite idiom, already the worker
   convention in `.supervisor-lane/STATE.md`) and writes only `candidates/` + one bus card.
2. **Money-path knowledge needs money-path evidence.** A lesson or decision touching the
   order path enters canon only citing its verification artifacts (commits, events, audit
   verdicts) — the standing precedent is
   `bhiksha/docs/lessons/money-path-audit-rounds-catch-different-bug-classes.md` (a green
   suite is not proof; audits are). The bus card must show those citations or you park it.
3. **The gate protects the prompt, not just the truth.** Brain files are injected into
   every future session — the curation gate is also the defense against wrong or
   manipulative instructions becoming ambient context. Nothing auto-appends to Layer 0/1.
4. **Failure posture: stale beats wrong.** Bus down / steward dead / you busy → canon
   simply ages, visibly (freshness lint), and sessions fall back to primary sources. No
   auto-promotion on timeout, ever.

## 6. Smallest real first step (one evening, by hand — no automation yet)

1. Write `mala_v2/CLAUDE.md` + `docs/brain/{INDEX,ARCHITECTURE,OPERATIONS,DECISIONS,STATE}.md`
   by **promoting the existing embryo**: the memory dir's system-knowledge notes
   (trading-loop-architecture, live-experiment-status, profile-coverage-gap…), the
   workplan status board, §11b vehicle policy, the ops facts (oldmac paths, bus idioms).
2. Slim the Claude memory dir to personal facts + a pointer ("system knowledge → repo brain").
3. Add the bhiksha `CLAUDE.md` stub.
4. **Verify against the surface that matters:** open a *fresh* session in mala_v2 and ask
   the acceptance question with no other context: *"Why is the OI floor 50 and what would
   change it?"* Pass = it answers: lowered 100→50 on 2026-07-08 as vehicle-policy phase 1
   after selector-empty blocked SMH/AMD live entries; phase 2 replaces it with per-symbol
   percentile OI learned from the chain snapshots accruing since 07-09, keeping a low
   absolute sanity bound — citing `docs/VEHICLE_POLICY_DECISION.md` / workplan §11b.

## 7. Growth path

- **Phase 1 — the brain exists (evening 1 + a few days' polish).** Success: a 5-question
  fresh-session quiz (OI floor; exit-authority rule; where does bhiksha run and how do I
  read its db safely; what gates a money-path change; what's queued next) answered
  correctly from bootstrap alone, in both repos, by Claude *and* by one non-Claude agent
  via AGENTS.md.
- **Phase 2 — the brain stays current without you initiating (week 2).** Steward job live
  on oldmac (broker + launchd), nightly card, freshness lint green. Success: after two
  weeks, `STATE.md`/`INDEX.md` reflect reality with ≤48h lag, every canon change has a bus
  receipt, and a session that reads only the brain makes zero stale claims about deploy
  state (spot-audited against `git log` on oldmac).
- **Phase 3 — the brain reaches beyond the repo (when wanted).** Steward additionally
  `mc capture`s a daily digest into a trading namespace so Jarvis/phone answers "what
  changed in the trading system this week?"; durable cross-agent facts go through mc's
  own propose→approve gate. Success: the phone answer matches the brain. Optional:
  retrieval upgrades only if grep provably fails.

## 8. Honest limits

- **No resident mind.** Between sessions nothing thinks; scheduled cognition (the
  steward) is the closest thing, and it runs in bounded bursts. "The repo becomes
  intelligent" truthfully means: *the repo becomes the memory and contract that any
  visiting intelligence instantly inhabits.* The building doesn't think — it has
  perfect handover notes, and that is what compounds.
- **Learning bandwidth = your curation bandwidth.** The gate that keeps canon true also
  rate-limits it. The design accepts staleness over noise; expect the brain to trail
  reality by a day, not lead it.
- **Today's cadence automation is fragile** — the watch/build cron jobs are session-scoped
  and expire (~07-13, per workplan). Phase 2's launchd+broker steward is the durable
  replacement for the *learning* loop specifically.
- **If you later want an always-on agent:** the natural path is an OpenClaw agent whose
  workspace is the mala_v2 checkout, allowlisted (the hard gate, per `openclaw-ops`) to
  the brain skill + mc + bus, woken by launchd or events. Everything in this RFC —
  brain files, bootstrap contract, curation gate — is exactly what that agent would
  inhabit. Building the substrate first is the no-regret move.

## 9a. Operator decisions (2026-07-09 evening, in-conversation)

- **Q1 curation — DECIDED: auto-commit, advisory curation.** "Auto commit is the right way,
  I do not want to become the bottleneck on a document that was written by an agent.
  However the same idea holds that I can really help with curation." Design consequence:
  the steward auto-commits ALL brain layers (STATE, DECISIONS, ARCHITECTURE, INDEX); the
  Lathi-bus card becomes a periodic advisory **curation digest** (weekly, alongside the
  scorecard) where pointy-bracket comments prune/correct canon after the fact — operator
  input improves the brain but never blocks it.
- **Q3 privacy — DECIDED by rule + verified empirically: stays in memory_core for now.**
  Operator rule: "either way is fine if the repo is private; if not we will keep it in
  memory_core." Verified 2026-07-09: **both mala_v2 and bhiksha are PUBLIC** on GitHub
  (skishore1676/*, isPrivate=false). Therefore personal P&L/trading-DNA → memory_core, as
  the RFC assumed. Mitigating facts found during verification: mala_v2 is 33 commits ahead
  of origin — the live-loop diary/P&L has never been pushed (local-only cadence protected
  it); bhiksha IS pushed publicly (code + lessons). Standing recommendation to operator:
  flip both repos private (they encode live strategy edge), which dissolves the partition
  and lets the brain hold everything; until then the public/private boundary is enforced
  by content type, and mala_v2 pushes stay paused or diary-free.

**2026-07-09 late: ALL questions resolved.** Q2 steward home = oldmac launchd (default
accepted); Q4 = via Pulsar (see below); Q5 naming = `docs/brain/` accepted; **home repo =
mala_v2, decided** — decisive rationale: bhiksha's checkout IS the production runtime
(deploy-gated, audited money path) and knowledge churn must never ride it; the embryo,
session entry points, and system-level scope all live in mala_v2. Division of labor:
bhiksha/docs/lessons/ stays code-adjacent, the brain indexes it by reference. RFC is
CLEARED FOR PHASE 1 (one-evening scaffold + bootstrap test), scheduled as the next build
increment after the 07-10 Friday synthesis.

## 9. Open questions (only you can answer)

1. ~~RESOLVED (see §9a)~~ **Curation appetite:** one nightly "Brain update" card during active weeks — will you
   review it, or should `STATE.md` auto-commit and only DECISIONS/ARCHITECTURE/INDEX gate?
2. **Steward home:** oldmac launchd against `~/Documents/mala_v2` (always-on; needs a
   headless Claude token per the agent-broker README) — approved? Or keep it dev-Mac,
   accepting sleep/expiry fragility?
3. ~~RESOLVED (see §9a)~~ **Privacy boundary:** may operator-personal material (your P&L narrative, trading DNA)
   live in the repo brain (mala_v2 pushes to GitHub), or must personal facts stay in
   memory_core/memory-dir with the brain holding only system knowledge? (RFC assumes the
   latter.)
4. ~~RESOLVED (operator, 2026-07-09)~~ **Phase 3 reach:** phone digest goes through
   **Pulsar** (the companion, ~/code/companion-pack) via its governed path — an mc
   namespace and/or a companion-pack lane consuming bus cards. Jarvis is deprecated;
   `jarvis-northstar` survives only as a legacy-named lathi-bus profile delivered by the
   Beacon bot. Namespace naming lands with phase 3 design.
5. **Naming:** `docs/brain/` (visible, operator-readable — recommended) vs `.intelligence/`
   (hidden). Any objection to `brain`?
