---
name: aidp-jira
description: Load Jira Cloud issue data into a Spark DataFrame from an AIDP notebook using the REST API v3 search/jql endpoint. Use when the user mentions Jira, Jira Cloud, issues, or JQL. Read-only; supports full and incremental loads.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-jira`: Jira Cloud issues to Spark

## When to use
- The user wants Jira Cloud issues in an AIDP notebook or Delta table.
- Not for writing to Jira, webhooks, or Jira Server/Data Center (on-prem).

## Prerequisites
1. Upload `connectors/jira/jira.py` to the workspace; add its folder to `sys.path`.
2. Environment variables `JIRA_SITE`, `JIRA_EMAIL`, `JIRA_API_TOKEN` set for the cluster (token from https://id.atlassian.com/manage/api-tokens). Never put the token in a cell.

## Use
Start from `connectors/jira/examples/jira_issue_load.ipynb`. The core calls:

    site, email, token = j.credentials_from_env()
    session = j.jira_session(email, token)
    issues = j.search_issues(session, site, query="project = KAN", since=since, until=until)
    df = j.to_dataframe(spark, list(issues))

## Gotchas
- Paging uses the server's opaque `nextPageToken` only. Never build or assume an offset — the old `/rest/api/3/search` endpoint (offset-based) was sunset by Atlassian on 31 October 2025 and no longer works.
- A caller `query` must not contain `ORDER BY`; paging supplies its own `ORDER BY updated ASC, key ASC`.
- **Verified 2026-09-29 live:** the maximum `maxResults` is exactly 5000; a value above it is rejected with a precise `HTTP 400` error, not silently capped.
- **Verified 2026-09-29 live:** a JQL query with no restricting clause is rejected outright (`Unbounded JQL queries are not allowed here`). This connector never hits it, because `search_issues` always includes an `updated <= "<until>"` bound.
- **Verified 2026-09-29 live:** `updated`/`created` are returned in the requesting account's configured timezone, not always UTC (observed `+0500`). The parser handles any offset, but don't assume UTC downstream.
- **v1 limitation, verified live:** custom fields (`customfield_NNNNN`) are not in the typed output. Requesting one via `fields=` does not add a column to the DataFrame.
- A watermark does not detect Jira issue deletes, so deleted issues stay in the target.
- Issues updated after `until` while a read runs are picked up by the next run; the overlap window re-reads a few issues and MERGE on `key` de-duplicates them.
- No live 429 was observed during testing; the retry path is verified only by offline unit tests. Rate-limit headers (`X-Ratelimit-Limit`/`X-Ratelimit-Remaining`) are present and decrement per request.
- Full verified-fact detail: `connectors/jira/spike/RESULTS.md` and `connectors/jira/CLAUDE.md`.
