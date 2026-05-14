# Prompt — Mala 2.2 Phase 0 Builder Context

Use this prompt to start a fresh Codex agent in `/Users/suman/code/mala_v2`.

```text
You are working in /Users/suman/code/mala_v2.

Goal:
  Help complete Phase 0 for Mala 2.2, focused on the first playbook:
  "Mean Reversion at Extremes."

Read first:
  - AGENTS.md
  - agent.md
  - docs/MALA_VISION_v0.2.md
  - research/playbooks/mean_reversion_at_extremes_v0.md

Important framing:
  - This is Mala 2.2, not a greenfield rebuild.
  - The product is an operator-bias-conditioned playbook surface.
  - Suman brings the market bias; Mala conditions that bias against historical evidence.
  - The system should eventually propose rule packets, but Phase 0 is only specification.
  - Do not build Bhiksha integration.
  - Do not build option overlay yet.
  - Do not publish to Google Sheets.
  - Do not create a broad playbook registry.
  - Do not turn this into autonomous strategy discovery.

Your Phase 0 job:
  1. Review the vision and playbook spec.
  2. Identify what decisions are still missing before any code should be written.
  3. Propose a concise Phase 0 decision checklist for Suman:
     - final 2-3 symbols
     - horizon
     - directions
     - Tier 1 trader-visible features
     - Tier 2 candidate features, if any
     - invalidation feature families
     - chart visualization requirement
  4. Do not implement code unless Suman explicitly approves the completed Phase 0 spec.

Deliver:
  - brief summary of the intended architecture
  - open decisions for Suman
  - recommended default choices
  - exact acceptance criteria for Phase 0 completion
  - proposed Phase 1 builder prompt, but do not start Phase 1

Tone:
  Be concise, direct, and skeptical of scope creep.
  If a requested feature pulls Bhiksha/options/runtime work into Phase 0, call that out.
```
