# oracle-aidp-connectors: project instructions

Independent Arbisoft project. Live-tested ingestion recipes for Oracle AI Data
Platform (AIDP) Workbench, delivered as a Claude Code plugin. Not affiliated with
or endorsed by Oracle. Read `docs/specs/2026-09-28-oracle-aidp-connectors-design.md`
before changing anything; it is the source of truth for scope and layout.

## Process

- The spec must be approved and an implementation plan written before any product
  code or scaffolding. Do not start building on your own.
- Each connector lives in `connectors/<source>/` with its own `CLAUDE.md` and
  `REQUIREMENTS.md`. Read both before working on that connector.
- Spike first: answer the connector's open questions against a real endpoint
  before writing its helper. Record each answer as a verified fact.
- **Probe identity/auth first**, before the connector's main resource (e.g.
  Jira's `/rest/api/3/myself` before `/search/jql`). The identity endpoint is
  usually the cheapest call, confirms auth works before anything else is
  built on top of it, and can surface facts (timezone, tenant metadata) the
  main resource's spike questions never think to ask.
- **One test request before any bulk operation while live-debugging**,
  especially around auth. A wrong password retried in a loop can trip a
  lockout or rate limit that then blocks even the correct one.
- One connector per pull request.

## Definition of done (every connector)

Implemented, unit-tested offline, run against a real endpoint from a live AIDP
workspace, recorded as a dated PASS in `connectors/<source>/live-results/`, and
documented with the AIDP-specific gotchas found. Without a live PASS it is
experimental and is not listed as supported.

## Rules

- Never print, log, or commit credentials, tokens, connection strings, instance
  URLs, hostnames, IP addresses, or OCIDs. Use placeholders in examples and
  results. Redact before logging.
- Never mark a live-test row PASS unless it actually ran. Use NOT RUN with the
  reason.
- Label every technical claim about AIDP or a source system as **verified**
  (observed in a live run) or **unverified** (from memory or docs). Do not write
  unverified claims into a skill as facts.
- Keep skills thin. Logic lives in `connectors/<source>/` and is unit-tested.
- Unit tests are offline: no network, no Spark. Mock the transport.
- Do not add a dependency the AIDP cluster does not already have unless the
  connector cannot work without it.
- Extract shared code only when a second connector needs the same thing.
  Shared infra lives in `connectors/_shared/` (`aidp_http.py` for the retry
  engine, `aidp_secrets.py` for credential resolution, `aidp_jars.py` for
  runtime JAR loading) — a notebook uploads it alongside the connector's own
  file, following Oracle's own connectors-plugin pattern of one shared
  package referenced by every connector.
- Never name a shared module after a Python standard library module (e.g.
  `secrets.py`, `http.py`) — once its directory is on `sys.path`, it silently
  shadows the real one for anything else that imports it. Prefix with `aidp_`
  or similar instead.
- Runtime contract: Spark 3.5, Python 3.11, Java 17. Write code that runs there.
- Do not copy code from Oracle's repositories without checking its licence and
  adding attribution. Default is to write our own following the same patterns.
- Test data is throwaway or vendor-sample only. No client data in this repo.
