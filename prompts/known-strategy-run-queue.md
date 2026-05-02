# Known Strategy Run Queue

Run these one at a time with Sonnet. Let each finish and inspect its `FINDINGS_REPORT.md` before starting the next.

Recommended order:

1. `research/hypotheses/jerk-pivot-current-basket-discovery.md`
2. `research/hypotheses/kinematic-ladder-current-basket-discovery.md`
3. `research/hypotheses/compression-breakout-current-basket-discovery.md`
4. `research/hypotheses/regime-router-current-basket-discovery.md`
5. `research/hypotheses/opening-drive-v2-current-basket-discovery.md`
6. `research/hypotheses/opening-drive-current-basket-discovery.md`

Command template:

```bash
cd /Users/suman/kg_env/projects/mala_v2
git pull --ff-only
claude --model sonnet "$(cat prompts/run-single-hypothesis-discovery.md)

Hypothesis file: research/hypotheses/<FILE>.md"
```

Notes:

- Use `sonnet` for all full M1-M5 runs.
- Use `haiku` only for post-run summarization when no commands or code edits are needed.
- Do not start the next hypothesis until the current one writes `FINDINGS_REPORT.md`.
- If the agent finds a code bug, pause the queue and fix the workbench before continuing.
