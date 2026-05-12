# Mala publication guardrails

Current canonical publish surface:

- `Mala_Evidence_v1` is the only Mala-owned evidence publication tab.
- `active_strategy` is mutated only by `research_ops shadow-activation-packet --apply-active-strategy` after explicit operator approval for activation.
- Legacy `Strategy_Catalog` is historical/read-only. Do not use it as a current publish target.

Preflight rule:

- Any command that mutates Google Sheets must verify the exact spreadsheet id and exact tab name before writing.
- Missing or renamed tabs must fail before mutation; commands must not auto-create a replacement publish tab.

Audit/validation rule:

- Audit and validation runs are read-only by default. Use dry-run/preview flags (`--dry-run`, omit `--apply`, omit `--publish-sheets`) unless the task explicitly says to mutate a named surface.
- Review Inbox generated-output hardening belongs in `workspace-main`; Mala-side audits should document the required `--no-write` workspace-main follow-up rather than editing generated Review Inbox surfaces from this repo.
