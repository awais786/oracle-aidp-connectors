# Jira Cloud connector

Read Jira Cloud issues into Spark on AIDP with `jira.py` (only `requests`).
`jira.py` imports the sibling `connectors/_shared/` package (HTTP retry
engine, credential resolution) — upload both to the same workspace folder.

Setup: see the skill `skills/aidp-jira/SKILL.md` and the example notebook in `examples/`.
Requirements and acceptance criteria: `REQUIREMENTS.md`. Verified API behaviour: `spike/RESULTS.md`.
Live-test status: `live-results/RESULTS.md`. Not supported until a PASS row exists there.

## Test

    pip install -r ../../requirements-dev.txt
    pytest -q
