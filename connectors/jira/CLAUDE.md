# Jira Cloud connector: instructions

Loaded when working in `connectors/jira/`. Project-wide rules are in the root
`CLAUDE.md`; what the connector must do is in `REQUIREMENTS.md`.

## Scope

Read-only ingestion of Jira Cloud issues into a Spark DataFrame from an AIDP
notebook, using the REST API v3. No writes, no webhooks, no OAuth in v1.

## Configuration

Credentials come only from environment variables (or the AIDP credential store
once verified):

- `JIRA_SITE`: site host, e.g. `awaisq.atlassian.net` (no scheme, no path)
- `JIRA_EMAIL`: the Atlassian account email
- `JIRA_API_TOKEN`: an API token from https://id.atlassian.com/manage/api-tokens

Never hard-code these, never put them in a notebook cell output, never commit a
real site name. Examples use `<site>.atlassian.net`.

## API facts (unverified until a spike run confirms them)

- Auth: HTTP Basic, `email:api_token` base64-encoded. This is Atlassian's own
  documented method for scripts (developer.atlassian.com, "Basic auth for REST
  APIs"), not deprecated as of 2026-09-29.
- Search endpoint: `POST https://<site>/rest/api/3/search/jql` with a JSON body
  `{"jql": "...", "fields": [...], "maxResults": N, "nextPageToken": "..."}`.
  The older `GET/POST /rest/api/3/search` (offset/`startAt` pagination) was
  fully sunset by Atlassian by 31 October 2025 and no longer works.
- Pagination: cursor-based via `nextPageToken` in the response; there is no
  reliable `total` count on this endpoint. The response omits `nextPageToken`
  when `isLast` is true.
- Default `maxResults` is 50 if unset; the real maximum is unverified — probe it
  by requesting a large value and reading back what the server actually returns.
- Rate limiting: expect HTTP 429; check for `Retry-After`.

When a spike confirms or refutes one of these, update this file and mark the
fact verified with the date.

## Working rules

- Use `requests` only. Do not add the Atlassian Python SDK.
- Page with `nextPageToken` only. Never assume or compute an offset.
- Incremental loads use JQL `updated >= "<timestamp>" ORDER BY updated ASC, key
  ASC` inside a fixed-until window, the same pattern as the ServiceNow
  connector: fix an upper bound per run, re-read a small overlap on the next
  run's lower bound, de-duplicate by issue key.
- Reject a caller-supplied JQL fragment that already contains `ORDER BY` —
  paging adds its own ordering, and two `ORDER BY` clauses conflict.
- Retry on 429 with `Retry-After`, bounded. Every request has a timeout.
- Unit tests use mocked HTTP. Never call a real site from a unit test.
- Live tests force a small `maxResults` so paging is exercised.

## Files (created once the plan is approved)

```
connectors/jira/
  CLAUDE.md          this file
  REQUIREMENTS.md    what it must do, acceptance criteria
  README.md          setup, usage, gotchas (written from live results)
  jira.py            helper module
  tests/             offline unit tests
  examples/          notebooks
  live-results/      dated result rows (placeholders only, no real site name)
```
