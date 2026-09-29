# oracle-aidp-connectors

Independent project by Arbisoft. Not affiliated with or endorsed by Oracle. Oracle and AI Data Platform are trademarks of Oracle Corporation.

Live-tested ingestion recipes for Oracle AI Data Platform (AIDP) Workbench, delivered as a Claude Code plugin. Each connector is a skill backed by a small unit-tested helper, run against a real endpoint from a live AIDP workspace, with the gotchas found written down.

| Connector | Status |
|---|---|
| Jira Cloud | In development. Not listed as supported until a live PASS is recorded. |

## How it works

A plugin is instructions plus code — nothing runs on its own until someone
asks for it. Two things happen in two different places:

```
"load Jira issues updated since yesterday"
        │
        ▼
Claude Code matches the aidp-jira skill (skills/aidp-jira/SKILL.md)
        │  the skill tells Claude what to upload and set up
        ▼
Claude uploads jira.py + connectors/_shared/ to the AIDP workspace,
sets JIRA_SITE/JIRA_EMAIL/JIRA_API_TOKEN, opens the example notebook
        │
        ▼
The notebook runs ON THE AIDP CLUSTER, not inside Claude Code:
  jira.py → search_issues() → to_dataframe() → Delta write / MERGE
        │
        ▼
A queryable table in AIDP
```

Claude Code and the skill handle the conversational "figure out what to do"
part. The plugin's own Python code, running on the AIDP cluster, handles the
actual data movement — that code is unit-tested and, for Jira, already
live-verified against a real site independently of any AIDP access.

## Install as a Claude Code plugin

From a local clone:

```
/plugin marketplace add /path/to/oracle-aidp-connectors
/plugin install oracle-aidp-connectors@oracle-aidp-connectors
```

*(Verified: `claude plugin validate .` passes, and both commands above were
run locally end to end — marketplace add and install both succeeded.)*

Then ask Claude, e.g. "load Jira issues updated since yesterday" — the
`aidp-jira` skill routes the request. Each skill's own prerequisites (what to
upload to the AIDP workspace, which environment variables or Vault secrets to
set) are in `skills/aidp-<source>/SKILL.md`.

## Layout

```
connectors/
  _shared/      # cross-connector infra (HTTP retry, credentials, jar loading)
  <source>/     # one connector: helper module, tests, example notebook,
                # spike results, live-test results
skills/         # one thin skill per connector, for plugin discovery
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
