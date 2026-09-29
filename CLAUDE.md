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
- One connector per pull request.

## Definition of done (every connector)

Implemented, unit-tested offline, run against a real endpoint from a live AIDP
workspace, recorded as a dated PASS in `tests/live-results/`, and documented with
the AIDP-specific gotchas found. Without a live PASS it is experimental and is
not listed as supported.

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
- Runtime contract: Spark 3.5, Python 3.11, Java 17. Write code that runs there.
- Do not copy code from Oracle's repositories without checking its licence and
  adding attribution. Default is to write our own following the same patterns.
- Test data is throwaway or vendor-sample only. No client data in this repo.
