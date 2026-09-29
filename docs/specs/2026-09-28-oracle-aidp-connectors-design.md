# oracle-aidp-connectors: Design

Date: 2026-09-28 · Status: draft for review

## Purpose

`oracle-aidp-connectors` is an independent collection of **live-tested AIDP
ingestion recipes** for source systems not currently covered by the AIDP samples
we have reviewed.

Each connector is implemented as a Claude Code skill backed by small,
unit-tested Python helpers. The skill gets a source dataset into an AIDP Spark
notebook and documents the AIDP-specific configuration, compatibility issues,
runtime behavior, and gotchas discovered during live testing.

The goal is not to build a generic connector framework or duplicate Oracle's
existing connector plugin. The value is the **tested recipe and the knowledge
captured from running it on AIDP**.

The collection is source-agnostic. Any source can be added if it meets the
definition of done below. **Jira Cloud** is the first connector: it tests
REST-based ingestion, HTTP Basic auth with an API token, cursor pagination,
rate limiting, watermark-based incremental reads, and API-to-Spark
normalization.

The project remains independent of `oracle-samples/oracle-aidp-samples`;
upstream contribution can be considered later after the recipes have been
proven.

**Coverage check (2026-09-28).** A search of `oracle-aidp-samples` for "jira"
across notebooks, Markdown, Python and JSON found no match. Its read-only
connector folder covers Fusion BICC, Kafka, MySQL HeatWave, NetSuite,
PeopleSoft, Siebel, REST, Salesforce and Snowflake. This checks the repository
files only; it does not prove AIDP has no native type for a source, so every
connector's first spike step confirms that against Oracle's connector
documentation.

## Definition of done (every connector)

A connector is **shipped** only when it is:

1. implemented;
2. unit-tested offline;
3. executed against a real endpoint from a live AIDP workspace;
4. recorded with a dated PASS result in `tests/live-results/`;
5. documented with the AIDP-specific gotchas discovered during the run.

A connector that fails 3 or 4 is marked NOT RUN with the reason and is treated
as experimental; the README does not list it as supported.

## Intent and success criteria

- A colleague or client can install the plugin, tell Claude "load Jira issues
  updated since yesterday", and get a Spark DataFrame in an AIDP notebook.
- Known problems are written into each skill, not left for users to rediscover.
- Adding the next connector means following this spec, not redesigning it.

Assumptions (from the requester, not verified against clients):

- Arbisoft engineers and clients use AIDP and need data from these systems.
- Notebooks run on the AIDP runtime contract: Spark 3.5, Python 3.11, Java 17.
- Testing uses free public endpoints (a free, self-service Jira Cloud site).
  No client data enters this repo.
- No pull requests to Oracle. The layout follows Oracle's connectors plugin so
  upstreaming stays possible later.

## Non-goals (v1)

- Writing data back to a source system.
- Private-network sources. The cluster's pods have no route into customer VCNs
  without admin-configured peering, so v1 targets public TLS endpoints only.
- Transformation, medallion pipelines or any other layer above ingestion.
- A general pipeline or connector framework.
- Jira Cloud OAuth 2.0 (3LO), webhooks, and writing issues.

## Repository layout

```
CLAUDE.md                     # project-wide instructions
.claude-plugin/plugin.json, marketplace.json
skills/
  aidp-<source>/SKILL.md      # one thin skill per connector (plugin discovery)
connectors/
  <source>/                   # everything for one connector lives here
    CLAUDE.md                 # connector-specific instructions
    REQUIREMENTS.md           # what it must do, acceptance criteria
    README.md                 # setup, usage, gotchas
    <source>.py               # helper module
    tests/                    # offline unit tests, no Spark and no network
    examples/                 # notebooks
    live-results/             # dated result rows
docs/specs/
README.md  CHANGELOG.md  TESTING.md  LICENSE
```

The only connector file outside `connectors/<source>/` is its skill, because the
Claude Code plugin discovers skills from `skills/`. Whether the helper can be
imported from `connectors/<source>/` inside a notebook, or must be packaged
under a single Python package, is settled in the scaffold step.

Rules:

- One connector per pull request, each with its own live-result row.
- Skills stay thin. Logic lives in `connectors/<source>/` and is unit-tested.
- Shared modules (credential lookup, runtime jar loading) are extracted only when
  a second connector needs the same code.
- The README carries: "Independent project by Arbisoft. Not affiliated with or
  endorsed by Oracle. Oracle and AI Data Platform are trademarks of Oracle
  Corporation."

## Shared conventions

**Credentials.** Helpers resolve a credential from an environment variable or the
AIDP credential store and return the value; they never print it. Any option
dictionary or URL containing a credential is redacted before it reaches a log
line or exception message.

**Runtime jars (for connectors that need one).** The cluster has no extra
drivers pre-installed. A jar-based connector downloads a pinned Maven artifact
set, verifies each file's SHA-256 against a pinned value, registers it with a
`URLClassLoader`, and calls `spark._jsc.addJar` so executors get it. This follows
the pattern documented for PostgreSQL and S3 in Oracle's plugin notes.

## Jira Cloud connector

Transport: the Jira Cloud REST API v3 search endpoint
(`POST /rest/api/3/search/jql`) over HTTPS, read with `requests` and
materialised as a DataFrame. The older `GET/POST /rest/api/3/search`
(offset/`startAt` pagination) was fully sunset by Atlassian by 31 October 2025;
`search/jql` with `nextPageToken` pagination is the current endpoint, verified
live against a real site on 2026-09-29 (see `connectors/jira/spike/RESULTS.md`).

`jira.py` provides:

- `jira_session(email, api_token)`: a `requests.Session` with HTTP Basic auth
  (`email:api_token`) — Atlassian's own documented method for scripts, distinct
  from the OAuth 2.0 (3LO) flow required for distributed apps, which is out of
  scope here since this is a single organisation's own script against its own
  site. Verified live: a real API token was accepted on the first try, with no
  MFA or Basic-Auth restriction blocking it.
- `account_timezone(session, site)`: the searching account's IANA timezone
  name, from `GET /rest/api/3/myself`. Added after a whole-branch review — see
  the timezone finding below.
- `search_issues(session, site, *, query, fields, since, overlap_seconds,
  until, page_size, tz_name)`: a generator that pages via `nextPageToken` only
  (never an offset), retries on HTTP 429 honouring `Retry-After`, and enforces
  a timeout. Rejects a caller `query` containing `ORDER BY`, since paging
  supplies its own `ORDER BY updated ASC, key ASC`. `fields` defaults to every
  `TYPED_FIELDS` column when omitted, not just `key`/`updated` — omitting it
  used to silently null every other column (found by review, fixed).
- Incremental loads: the JQL gains `updated >= "<lower>" AND updated <= "<T0>"`
  ANDed onto the caller's filter, expressed in the account's own timezone (see
  below) — fix an upper bound per run, re-read a small overlap on the next
  run's lower bound, de-duplicate by issue key. Jira's `nextPageToken` is an
  opaque cursor Atlassian manages, not a value the caller constructs, so there
  is no keyset-ordering problem to solve on the way in.
- `to_dataframe(spark, rows)`: typed columns for common issue fields (`key`,
  `summary`, `status`, `priority`, `assignee`, `reporter`, `created`, `updated`,
  `issuetype`, `project`), plus a trailing `raw_fields` JSON-string column
  holding any other requested field (e.g. a custom field) rather than
  silently dropping it — found missing by review, fixed.

**Verified live (2026-09-29), see `connectors/jira/spike/RESULTS.md` and
`connectors/jira/CLAUDE.md` for full detail:**
- `search/jql` pagination behaves exactly as documented: lossless, `isLast`
  and `nextPageToken` behave correctly.
- `maxResults` maximum is exactly 5000; a value above it is rejected with a
  precise error, not silently capped.
- A JQL query with no restricting clause is rejected outright — this connector
  never hits it, because `search_issues` always includes an `updated <=
  "<until>"` bound.
- No live 429 was triggered during testing; the retry path is verified only by
  offline unit tests. Rate-limit headers (`X-Ratelimit-Limit`/
  `X-Ratelimit-Remaining`) are present and decrement per request.

**Timezone finding, corrected after a whole-branch review (2026-09-29).** The
spike found `updated`/`created` are returned in the requesting account's
configured timezone, not UTC (observed `+0500`), and originally concluded "no
code change needed" — that only checked *reading* timestamps back. It missed
that Jira also interprets the JQL date-time literals `search_issues` *sends*
in that same account timezone, not UTC. Sending UTC-formatted bounds to a
non-UTC account silently shifted the effective watermark by the account's
offset — for an account behind UTC, this **permanently and silently skipped**
issues updated in the gap. Fixed: `account_timezone()` plus a `tz`/`tz_name`
parameter on `format_jql_timestamp`/`build_jql`/`search_issues`, which the
notebook fetches once per run and passes explicitly. Omitting `tz_name`
defaults to UTC, which is only correct for a UTC account.

## Testing

- **Unit tests (offline, in CI).** Mocked HTTP covering paging, 429 backoff,
  watermark overlap, empty results and schema fallback; redaction tests for
  anything that handles credentials.
- **Live tests (manual, recorded).** Each example notebook runs on a live AIDP
  cluster. The result (status, row count, date, cluster runtime) goes into
  `RESULTS.md` and a `row<N>.json` artifact. A row that could not run is marked
  NOT RUN with the reason.
- Live tests use throwaway endpoints and synthetic or vendor-sample data only.

## Spikes (answered before each connector's build)

For every connector, first: does Oracle's AIDP connector documentation list a
native `aidataplatform` type for this source? If yes, the deliverable becomes a
tested recipe for that native type instead of a helper, and this spec is updated
before any code is written.

Jira Cloud, native-type check (answered 2026-09-29, see
`connectors/jira/spike/RESULTS.md`): no native Jira type found in Oracle's
connectors plugin README or a general web search of the `aidataplatform` type
list. Reasonably confident, not certain — Oracle's own blog page on external
connectors could not be fetched directly to confirm.

Jira Cloud, API spike (answered 2026-09-29, see `connectors/jira/spike/RESULTS.md`):
1. Does `POST /rest/api/3/search/jql` behave as documented against a real site
   (JQL + fields in the body, `nextPageToken` in the response)? — Yes.
2. What is the real maximum `maxResults`, and the rate-limit behaviour? —
   Exactly 5000; precise rejection above it; `X-Ratelimit-*` headers present.
3. How do custom fields, person objects, and timestamps appear, and in what
   timezone? — Person fields are dicts with `displayName`; timestamps are in
   the requesting account's timezone, not always UTC.
4. What does an invalid JQL string return? — HTTP 400 with an `errorMessages`
   array.

Each answer is recorded in the connector's skill as a documented fact, not left
as an assumption.

## Delivery order

0. Gates, checked before any code: a live AIDP workspace and cluster; a free
   Jira Cloud site (self-service at id.atlassian.com) and an API token
   (id.atlassian.com/manage/api-tokens).
1. Repo scaffold: plugin manifests, README with disclaimer, test harness. Done
   — commits on `main`. The licence itself is still an open item below; only
   the disclaimer text is in place.
2. Jira Cloud: spike (done, Proceed), helper, skill, example notebook (done,
   62 unit tests). A whole-branch review before merge found three Critical
   defects (fields defaulting to only `key`+`updated`, so the notebook loaded
   mostly-null rows; JQL watermark bounds sent in UTC while Jira reads them in
   the account's own timezone, which could silently skip issues; `pytest.ini`
   pointing at a removed connector, breaking CI) plus several Important
   findings (a real hostname committed against this repo's own rule; the
   native-type gate never answered; custom fields silently dropped instead of
   the documented JSON fallback). All fixed, each with a test written first;
   the full suite and `claude plugin validate .` both pass after the fix pass.
   Live AIDP run and RESULTS row (Task 8) still pending AIDP workspace access
   — not claimed as PASS.
3. Further connectors, chosen by demand from Arbisoft engineers and clients.
   Each follows the definition of done and gets its own spec section before it
   is built.

Separate pull requests within the repo for each connector. No calendar estimates
until each connector's own spike shows how its endpoint behaves.

## Risks

- **Reachability.** If the cluster cannot reach a source, its connector cannot
  be live-tested. Mitigation: the spike runs first; a connector that cannot be
  tested is marked NOT RUN and stays experimental.
- **Version drift.** AIDP runtime or connector releases can break the recipes.
  Mitigation: pinned versions, dated results, and a documented re-test procedure.
- **Free-tier limits.** A free Jira Cloud site's rate limits (200
  requests/interval observed) can affect an unattended run at volume.
  Mitigation: documented setup steps in `TESTING.md`.
- **API churn.** Atlassian has already sunset one search endpoint
  (`/rest/api/3/search`) in favour of `search/jql`. Mitigation: the spike
  reconfirms current behaviour live before any paging code is written, and the
  connector's skill is updated, not assumed correct from documentation alone.
- **Naming.** The name starts with "oracle-". See the README disclaimer; confirm
  with Arbisoft before making the repo public.
- **Licensing.** Oracle's plugin is MIT and its repository root is UPL. Any code
  copied from it needs attribution and a licence check; the default is to write
  our own following the same patterns.

## Open items

- Which GitHub organisation hosts the repo, and whether it is private or public.
  The current remote is a personal account (`awais786/oracle-aidp-connectors`).
- Licence for this repo (MIT is the default if it goes public).
