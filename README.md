# oracle-aidp-connectors

Independent project by Arbisoft. Not affiliated with or endorsed by Oracle. Oracle and AI Data Platform are trademarks of Oracle Corporation.

Live-tested ingestion recipes for Oracle AI Data Platform (AIDP) Workbench, delivered as a Claude Code plugin. Each connector is a skill backed by a small unit-tested helper, run against a real endpoint from a live AIDP workspace, with the gotchas found written down.

| Connector | Status |
|---|---|
| Jira Cloud | In development. Not listed as supported until a live PASS is recorded. |

Design: `docs/specs/2026-09-28-oracle-aidp-connectors-design.md`. Development rules: `CLAUDE.md`.

Licence: not yet chosen. Do not redistribute until one is added.

## Tests

    pip install -r requirements-dev.txt
    pytest -q
