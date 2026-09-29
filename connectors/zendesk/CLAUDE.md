# Zendesk connector: instructions

Loaded when working in `connectors/zendesk/`. Project-wide rules are in the
root `CLAUDE.md`; what the connector must do is in `REQUIREMENTS.md`.

Status: scope approved 2026-09-30, pre-spike. No code written yet.

## Scope

Read-only ingestion of Zendesk tickets into a Spark DataFrame from an AIDP
notebook, using the REST API v2 Incremental Exports endpoint. No writes, no
webhooks, no OAuth2 in v1.

## Configuration

`credentials_from_env()` (planned, same as Jira's) resolves each value below
through `connectors/_shared/aidp_secrets.py`: OCI Vault first (if
`OCI_VAULT_ID` is set), falling back to a plain environment variable.

- `ZENDESK_SUBDOMAIN`: subdomain only, e.g. `example` for
  `example.zendesk.com` (no scheme, no path)
- `ZENDESK_EMAIL`: the Zendesk account email
- `ZENDESK_API_TOKEN`: an API token from Admin Center → Apps and
  integrations → APIs → Zendesk API

Never hard-code these, never put them in a notebook cell output, never
commit a real subdomain. Examples use `<subdomain>.zendesk.com`.

## API facts — ALL UNVERIFIED until the spike runs

From Zendesk's public docs, not yet observed live. Don't treat any of this
as fact in a shipped README until `spike/RESULTS.md` confirms it.

- Auth: HTTP Basic, `{email}/token:{api_token}`, base64-encoded. Admin
  Center must enable API token access first — check this isn't a blocker
  before the spike.
- `GET /api/v2/incremental/tickets/cursor.json`: first call takes
  `start_time` (Unix epoch); each page returns `after_cursor` and
  `end_of_stream`; `per_page` up to 1000, default 1000.
- **Rate limit: 10 req/min on this endpoint (30 with the High-Volume
  add-on)** — far tighter than Jira's. Headers reportedly
  `Zendesk-RateLimit-incremental-exports-cursor: total={n}; remaining={n};
  resets={n}`, not Jira's generic `X-Ratelimit-*`. Confirm the header name
  and whether `Retry-After` also appears on a 429.
- `custom_fields`: documented as `[{id, value}]` — an array, not named keys
  like Jira's `customfield_NNNNN`. Needs its own `to_dataframe` test.
- Timestamps: docs suggest ISO 8601 with an explicit UTC offset, no known
  account-timezone quirk like Jira's — confirm live regardless, since
  Jira's own "no issue" assumption here was wrong until a review caught it.
- Sources: [Incremental Exports](https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/),
  [Rate limits](https://developer.zendesk.com/api-reference/introduction/rate-limits/).

## Working rules

- Use `requests` only. Page with `after_cursor` only, never an offset.
- Don't implement incremental reads until the spike settles
  REQUIREMENTS.md's F4 (timestamp watermark vs. persisted cursor).
- Retry on 429 per this endpoint's actual rate limit and header format,
  confirmed by the spike, not assumed from Jira's. Every request has a
  timeout.
- Unit tests use mocked HTTP, never a real site. Live tests force a small
  `per_page` so paging is exercised.

## Shared package (`connectors/_shared/`)

`zendesk.py` (planned) reuses Jira's sibling modules unchanged unless the
spike says otherwise:

- `aidp_http.py`: retry engine + `redact()`. May need a Zendesk-specific
  rate-limit header parser layered on top — decide after the spike.
- `aidp_secrets.py`: Vault → environment → default credential fallback,
  same shape as Jira (subdomain/email/token instead of site/email/token).

`aidp_jars.py` not needed — Zendesk is REST, no JDBC driver.

A notebook must upload **both** `zendesk.py` and the `_shared/` folder to
the same workspace path, exactly like Jira's notebook does.

## Files (created once the implementation plan is approved)

```
connectors/zendesk/
  CLAUDE.md          this file
  REQUIREMENTS.md    what it must do, acceptance criteria
  README.md          setup, usage, gotchas (written from live results)
  zendesk.py         helper module (imports connectors/_shared)
  tests/             offline unit tests
  examples/          notebooks
  live-results/      dated result rows (placeholders only, no real site name)
```
