---
name: aidp-jira
description: Load Jira Cloud issue data into a Spark DataFrame from an AIDP notebook using the REST API v3 search/jql endpoint. Use when the user mentions Jira, Jira Cloud, issues, or JQL. Read-only; supports full and incremental loads.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-jira`: Jira Cloud issues to Spark

## When to use
- The user wants Jira Cloud issues in an AIDP notebook or Delta table.
- Mentioned: "Jira", "Jira Cloud", "JQL", "issues", "sprint", "backlog".
- Not for writing to Jira, webhooks, or Jira Server/Data Center (on-prem —
  that's a different product with a different API).

## Prerequisites
1. Upload `connectors/jira/jira.py` **and** the sibling `connectors/_shared/` package to the same workspace folder; add that folder to `sys.path`. `_shared/` holds the HTTP retry engine and credential resolution `jira.py` imports.
2. Environment variables `JIRA_SITE`, `JIRA_EMAIL`, `JIRA_API_TOKEN` set for the cluster (token from https://id.atlassian.com/manage/api-tokens), or OCI Vault secrets with `OCI_VAULT_ID` set. Never put the token in a cell.

## Use

### Full load

```python
from datetime import datetime, timezone
import jira as j

site, email, token = j.credentials_from_env()
session = j.jira_session(email, token)
tz_name = j.account_timezone(session, site)  # Jira reads JQL times in this zone, not UTC

issues = j.search_issues(session, site, query="project = KAN", tz_name=tz_name)
df = j.to_dataframe(spark, list(issues))
df.write.format("delta").saveAsTable("<catalog>.<schema>.jira_issue")
```

### Incremental load (subsequent runs)

Read the watermark back from the target table rather than tracking it
separately — see `connectors/jira/examples/jira_issue_load.ipynb` for the full
notebook, including the `MERGE INTO` that de-duplicates on `key`.

```python
since = spark.sql("SELECT max(updated) AS m FROM <catalog>.<schema>.jira_issue").first()["m"]
until = datetime.now(timezone.utc)
issues = j.search_issues(
    session, site, query="project = KAN", tz_name=tz_name,
    since=since, until=until, overlap_seconds=300,
)
df = j.to_dataframe(spark, list(issues))
```

## Gotchas
- Paging uses the server's opaque `nextPageToken` only. Never build or assume an offset — the old `/rest/api/3/search` endpoint (offset-based) was sunset by Atlassian on 31 October 2025 and no longer works.
- A caller `query` must not contain `ORDER BY`; paging supplies its own `ORDER BY updated ASC, key ASC`.
- **Verified 2026-09-29 live:** the maximum `maxResults` is exactly 5000; a value above it is rejected with a precise `HTTP 400` error, not silently capped.
- **Verified 2026-09-29 live:** a JQL query with no restricting clause is rejected outright (`Unbounded JQL queries are not allowed here`). This connector never hits it, because `search_issues` always includes an `updated <= "<until>"` bound.
- **Verified 2026-09-29 live:** `updated`/`created` are returned in the requesting account's configured timezone, not always UTC (observed `+0500`). The parser handles any offset, but don't assume UTC downstream.
- Custom fields (`customfield_NNNNN`) and any other requested field outside the typed columns land in a trailing `raw_fields` JSON-string column rather than being dropped.
- A watermark does not detect Jira issue deletes, so deleted issues stay in the target.
- Issues updated after `until` while a read runs are picked up by the next run; the overlap window re-reads a few issues and MERGE on `key` de-duplicates them.
- No live 429 was observed during testing; the retry path is verified only by offline unit tests. Rate-limit headers (`X-Ratelimit-Limit`/`X-Ratelimit-Remaining`) are present and decrement per request.
- Full verified-fact detail: `connectors/jira/spike/RESULTS.md` and `connectors/jira/CLAUDE.md`.

## References
- Helper: [connectors/jira/jira.py](../../connectors/jira/jira.py)
- Shared HTTP/credentials/jar-loading code: [connectors/_shared/](../../connectors/_shared/)
- Example notebook: [connectors/jira/examples/jira_issue_load.ipynb](../../connectors/jira/examples/jira_issue_load.ipynb)
- Live-test handoff guide: [connectors/jira/LIVE_TEST_GUIDE.md](../../connectors/jira/LIVE_TEST_GUIDE.md)
- Verified API facts: [connectors/jira/spike/RESULTS.md](../../connectors/jira/spike/RESULTS.md)
- Official Jira Cloud REST API v3 docs: https://developer.atlassian.com/cloud/jira/platform/rest/v3/
