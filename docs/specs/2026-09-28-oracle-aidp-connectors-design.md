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
definition of done below. The first connector is **ServiceNow**, which tests
REST-based ingestion, authentication, pagination, rate limiting, watermark-based
incremental reads, and API-to-Spark normalization.

The project remains independent of `oracle-samples/oracle-aidp-samples`;
upstream contribution can be considered later after the recipes have been
proven.

**Coverage check (2026-09-28).** A search of `oracle-aidp-samples` for
"servicenow" across notebooks, Markdown, Python and JSON found no match. Its
read-only connector folder covers Fusion BICC, Kafka, MySQL HeatWave, NetSuite,
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

- A colleague or client can install the plugin, tell Claude "load ServiceNow
  incidents updated since yesterday", and get a Spark DataFrame in an AIDP
  notebook.
- Known problems are written into each skill, not left for users to rediscover.
- Adding the next connector means following this spec, not redesigning it.

Assumptions (from the requester, not verified against clients):

- Arbisoft engineers and clients use AIDP and need data from these systems.
- Notebooks run on the AIDP runtime contract: Spark 3.5, Python 3.11, Java 17.
- Testing uses free public endpoints (for ServiceNow, a Personal Developer
  Instance). No client data enters this repo.
- No pull requests to Oracle. The layout follows Oracle's connectors plugin so
  upstreaming stays possible later.

## Non-goals (v1)

- Writing data back to a source system.
- Private-network sources. The cluster's pods have no route into customer VCNs
  without admin-configured peering, so v1 targets public TLS endpoints only.
- Transformation, medallion pipelines or any other layer above ingestion.
- A general pipeline or connector framework.
- ServiceNow OAuth flows and attachments.

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

## ServiceNow connector

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

## Testing

- **Unit tests (offline, in CI).** For ServiceNow: mocked HTTP covering paging,
  429 backoff, watermark overlap, empty results and schema fallback; redaction
  tests for anything that handles credentials.
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

Each answer is recorded in the connector's skill as a documented fact, not left
as an assumption.

## Delivery order

0. Gates, checked before any code: (a) a live AIDP workspace and cluster are
   available; (b) a ServiceNow Personal Developer Instance has been obtained
   from developer.servicenow.com (Request Instance). Instances are reclaimed
   after 10 days of inactivity on the developer site, so the live run is
   scheduled shortly after the instance is requested.
1. Repo scaffold: plugin manifests, README with disclaimer and license, test
   harness.
2. ServiceNow: spike, helper, skill, example notebook, live run, RESULTS row.
3. Further connectors, chosen by demand from Arbisoft engineers and clients.
   Each follows the definition of done and gets its own spec section before it
   is built.

Separate pull requests within the repo for each connector. No calendar estimates
until the first spike shows how the cluster behaves.

## Risks

- **Reachability.** If the cluster cannot reach a source, its connector cannot
  be live-tested. Mitigation: the spike runs first; a connector that cannot be
  tested is marked NOT RUN and stays experimental.
- **Version drift.** AIDP runtime or connector releases can break the recipes.
  Mitigation: pinned versions, dated results, and a documented re-test procedure.
- **Free-tier limits.** Developer-instance reclamation and hibernation break
  unattended runs. Mitigation: documented setup steps in `TESTING.md`.
- **Naming.** The name starts with "oracle-". See the README disclaimer; confirm
  with Arbisoft before making the repo public.
- **Licensing.** Oracle's plugin is MIT and its repository root is UPL. Any code
  copied from it needs attribution and a licence check; the default is to write
  our own following the same patterns.

## Open items

- Which GitHub organisation hosts the repo, and whether it is private or public.
  The current remote is a personal account (`awais786/oracle-aidp-connectors`).
- Licence for this repo (MIT is the default if it goes public).
