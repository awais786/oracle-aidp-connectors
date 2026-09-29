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
definition of done below. **ServiceNow** was started first and is **paused**:
two fresh Personal Developer Instances both refused HTTP Basic auth for `admin`
(`401 User is not authenticated`), matching ServiceNow's Basic Auth Restriction
/ MFA-on-new-instances behaviour, and an OAuth Client Credentials attempt also
failed (`access_denied`), most likely because
`glide.oauth.inbound.client.credential.grant_type.enabled` is unset. Its code
(query builder, HTTP layer with retry) is committed and tested on the
`servicenow-connector` branch and can resume once the instance accepts API
auth. **Jira Cloud** is the active first connector: it tests REST-based
ingestion, HTTP Basic auth with an API token, cursor pagination, rate limiting,
watermark-based incremental reads, and API-to-Spark normalization.

The project remains independent of `oracle-samples/oracle-aidp-samples`;
upstream contribution can be considered later after the recipes have been
proven.

**Coverage check (2026-09-28).** A search of `oracle-aidp-samples` for
"servicenow" and "jira" across notebooks, Markdown, Python and JSON found no
match for either. Its read-only connector folder covers Fusion BICC, Kafka,
MySQL HeatWave, NetSuite, PeopleSoft, Siebel, REST, Salesforce and Snowflake.
This checks the repository files only; it does not prove AIDP has no native
type for a source, so every connector's first spike step confirms that against
Oracle's connector documentation.

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
- Testing uses free public endpoints (a ServiceNow Personal Developer Instance;
  a free, self-service Jira Cloud site). No client data enters this repo.
- No pull requests to Oracle. The layout follows Oracle's connectors plugin so
  upstreaming stays possible later.

## Non-goals (v1)

- Writing data back to a source system.
- Private-network sources. The cluster's pods have no route into customer VCNs
  without admin-configured peering, so v1 targets public TLS endpoints only.
- Transformation, medallion pipelines or any other layer above ingestion.
- A general pipeline or connector framework.
- ServiceNow attachments. ServiceNow OAuth was tried as a workaround for the
  Basic-auth block (see Purpose) and is a live finding, not a shipped feature;
  it stays out of the ServiceNow connector's v1 scope.
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

## ServiceNow connector (paused — see Purpose)

Transport: the ServiceNow Table API (`/api/now/table/<table>`) over HTTPS, read
with `requests` and materialised as a DataFrame.

`servicenow.py` provides:

- `servicenow_session(user, password)`: a `requests.Session` with Basic auth. The
  instance is passed to `fetch_table`, not stored on the session. OAuth is out of
  scope for v1.
- `fetch_table(session, instance, table, *, fields, query, since, display_value,
  page_size)`: a generator that pages through the table, sends
  `sysparm_exclude_reference_link=true`, retries on HTTP 429 honouring
  `Retry-After`, and enforces a timeout.
- Incremental loads: pass `since` (a `sys_updated_on` watermark). The caller
  stores the watermark; the reader re-reads a small overlap window and the
  caller de-duplicates on `sys_id`.
- `to_dataframe(spark, rows, table)`: a typed schema for `incident` and
  `change_request`; every other table falls back to a single JSON-string column,
  the same fallback Oracle's Fusion REST helper uses.

Paging design: keyset paging on `sys_id`, inside a fixed time window. Each run
fixes an upper bound `T0` at its start and reads
`sys_updated_on >= <lower>^sys_updated_on <= T0^sys_id > <last>^ORDERBYsys_id`,
where `<last>` is the final `sys_id` of the previous page. Every condition is an
AND, so encoded-query `OR` precedence never applies. Offset paging is rejected:
it is slow on deep pages, and if a row leaves the filter set mid-read (because it
was updated after `T0`) every later row shifts down by one and one is skipped. A
keyset is unaffected by that. A row updated after `T0` is missed by this run and
picked up by the next one. The caller's next lower bound is `T0`, and the overlap
window covers rows committed late.

The spike must prove that ServiceNow compares `sys_id` with `>` as a string and
orders by it. If it does not, stop and revise this section; do not build a
speculative fallback.

Other gotchas to verify and record: reference fields returned as `{link, value}`
objects versus plain values under `sysparm_display_value`, timezone of
`sys_updated_on`, table-level ACLs returning empty results instead of errors,
and developer-instance hibernation and reclamation.

## Jira Cloud connector

Transport: the Jira Cloud REST API v3 search endpoint
(`POST /rest/api/3/search/jql`) over HTTPS, read with `requests` and
materialised as a DataFrame. The older `GET/POST /rest/api/3/search`
(offset/`startAt` pagination) was fully sunset by Atlassian by 31 October 2025;
`search/jql` with `nextPageToken` pagination is the current endpoint as of
2026-09-29, verified against Atlassian's own developer documentation and
migration notices, and reconfirmed live in the spike before any paging code is
written.

`jira.py` provides:

- `jira_session(email, api_token)`: a `requests.Session` with HTTP Basic auth
  (`email:api_token`) — Atlassian's own documented method for scripts, distinct
  from the OAuth 2.0 (3LO) flow required for distributed apps, which is out of
  scope here since this is a single organisation's own script against its own
  site.
- `search_issues(session, site, *, jql, fields, since, overlap_seconds, until,
  page_size)`: a generator that pages via `nextPageToken` only (never an
  offset), retries on HTTP 429 honouring `Retry-After`, and enforces a timeout.
  Rejects a caller `jql` containing `ORDER BY`, since paging supplies its own
  `ORDER BY updated ASC, key ASC`.
- Incremental loads: the JQL gains `updated >= "<lower>" AND updated <= "<T0>"`
  ANDed onto the caller's filter, the same fixed-window-plus-overlap shape as
  ServiceNow's design, but without a keyset-ordering landmine to solve — Jira's
  `nextPageToken` is an opaque cursor Atlassian manages, not a value the caller
  constructs.
- `to_dataframe(spark, rows)`: typed columns for common issue fields (`key`,
  `summary`, `status`, `priority`, `assignee`, `reporter`, `created`, `updated`,
  `issuetype`, `project`); any other requested field is carried as a JSON
  string, the same fallback pattern as the ServiceNow and Fusion REST helpers.

Gotchas to verify and record: the real maximum `maxResults` (documented default
is 50), rate-limit headers, how `assignee`/`reporter` person objects and custom
fields (`customfield_XXXXX`) appear in the response, the timezone of `updated`,
and the response shape for an invalid JQL string.

## Testing

- **Unit tests (offline, in CI).** For ServiceNow and Jira: mocked HTTP covering
  paging, 429 backoff, watermark overlap, empty results and schema fallback;
  redaction tests for anything that handles credentials.
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

ServiceNow:
1. Does `sys_id > <last>` with `ORDERBYsys_id` page correctly (every row once,
   in order)? If not, revise the paging design before building.
2. What are the real page-size and rate-limit behaviours?
3. What do reference fields and timestamps look like with each
   `sysparm_display_value` setting?

Jira Cloud:
1. Does `POST /rest/api/3/search/jql` behave as documented against a real site
   (JQL + fields in the body, `nextPageToken` in the response)?
2. What is the real maximum `maxResults`, and the rate-limit behaviour?
3. How do custom fields, person objects, and timestamps appear, and in what
   timezone?
4. What does an invalid JQL string return?

Each answer is recorded in the connector's skill as a documented fact, not left
as an assumption.

## Delivery order

0. Gates, checked before any code:
   - ServiceNow: a live AIDP workspace and cluster; a Personal Developer
     Instance from developer.servicenow.com. **Currently blocked** — see
     Purpose. Instances are reclaimed after 10 days of developer-site
     inactivity.
   - Jira Cloud: a live AIDP workspace and cluster; a free Jira Cloud site
     (self-service at id.atlassian.com) and an API token
     (id.atlassian.com/manage/api-tokens).
1. Repo scaffold: plugin manifests, README with disclaimer and license, test
   harness. Done — commits on `main`.
2. ServiceNow: spike, helper, skill, example notebook, live run, RESULTS row.
   Paused after the query builder and HTTP layer (committed, tested, on branch
   `servicenow-connector`); resumes once the instance accepts API auth.
3. Jira Cloud (active): spike, helper, skill, example notebook, live run,
   RESULTS row.
4. Further connectors, chosen by demand from Arbisoft engineers and clients.
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
- **Free-tier limits.** ServiceNow developer-instance reclamation and
  hibernation, and any Jira Cloud free-site rate limits, can break unattended
  runs. Mitigation: documented setup steps in `TESTING.md`.
- **API churn.** Atlassian has already sunset one search endpoint
  (`/rest/api/3/search`) in favour of `search/jql`; a forum thread also
  questions whether `search/jql` itself is being changed. Mitigation: the
  spike reconfirms current behaviour live before any paging code is written,
  and the connector's skill is updated, not assumed correct from documentation
  alone.
- **Naming.** The name starts with "oracle-". See the README disclaimer; confirm
  with Arbisoft before making the repo public.
- **Licensing.** Oracle's plugin is MIT and its repository root is UPL. Any code
  copied from it needs attribution and a licence check; the default is to write
  our own following the same patterns.

## Open items

- Which GitHub organisation hosts the repo, and whether it is private or public.
  The current remote is a personal account (`awais786/oracle-aidp-connectors`).
- Licence for this repo (MIT is the default if it goes public).
