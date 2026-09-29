# oracle-aidp-connectors

Independent project by Arbisoft. Not affiliated with or endorsed by Oracle. Oracle and AI Data Platform are trademarks of Oracle Corporation.

Live-tested ingestion recipes for Oracle AI Data Platform (AIDP) Workbench.
Each connector is a small, unit-tested Python helper plus an example
notebook, run against a real endpoint from a live AIDP workspace, with the
gotchas found written down.

| Connector | Status |
|---|---|
| Jira Cloud | In development. Not listed as supported until a live PASS is recorded. |

## How it works

Nothing runs on its own — you upload a connector's helper module and run its
example notebook yourself, on your own AIDP cluster:

```
Upload jira.py + connectors/_shared/ to an AIDP workspace folder,
set JIRA_SITE/JIRA_EMAIL/JIRA_API_TOKEN, open the example notebook
        │
        ▼
The notebook runs ON THE AIDP CLUSTER:
  jira.py → search_issues() → to_dataframe() → Delta write / MERGE
        │
        ▼
A queryable table in AIDP
```

Each connector's `README.md` and `LIVE_TEST_GUIDE.md` walk through the exact
upload/setup steps. The code is unit-tested and, for Jira, already
live-verified against a real site independently of any AIDP access.

*(This repo previously shipped a Claude Code plugin/skill wrapper around
these connectors, for driving that upload/setup from a Claude Code chat. It's
been removed for now — no clear use case for it yet, since the connectors
work standalone and nothing about the actual data movement needs an LLM in
the loop. May revisit if that changes.)*

## Layout

```
connectors/
  _shared/      # cross-connector infra (HTTP retry, credentials, jar loading)
  <source>/     # one connector: helper module, tests, example notebook,
                # spike results, live-test results
docs/specs/     # the design this repo follows
```

## Docs

- Design: `docs/specs/2026-09-28-oracle-aidp-connectors-design.md`
- Development rules: `CLAUDE.md`
- How to test, including a local Spark+Delta check that needs no AIDP access:
  `TESTING.md`
- Changelog: `CHANGELOG.md`

Licence: not yet chosen. Do not redistribute until one is added.

## Tests

    pip install -r requirements-dev.txt
    pytest -q
