---
name: build-connector
description: Use when adding a new data-source connector to this repo — "build the X connector", "add a connector for X", picking up a candidate from GitHub issue #1. Drives spec approval, a live spike, an implementation plan, TDD build, and live-AIDP-test handoff, following this repo's established pattern.
---

# build-connector

## Overview

This repo ships AIDP ingestion connectors (Jira Cloud shipped first). Every
connector follows the same shape: a small, unit-tested Python helper that
reuses `connectors/_shared/`, a live-verified spike before any paging/query
code is written, a TDD implementation plan, and a handoff for the live AIDP
run — Claude has no AIDP workspace access, so that step is always manual.

**Core principle:** no product code before an approved spec; no paging or
query code before a live spike proves the real API's behavior, not just its
docs.

## When to use

- "Build/add/implement the `<source>` connector"
- Picking up a candidate from GitHub issue #1 ("Candidate connectors for
  future work")

Not for: editing `connectors/_shared/` in isolation, or fixing a bug in an
already-shipped connector — use `superpowers:systematic-debugging` for that.

## The gates, in order

1. **Scope check.** Grep `oracle-aidp-samples` for the source name — stop if
   Oracle's own native connectors already cover it. If uncertain, say so.
2. **Spec approved (hard gate).** Add a section to
   `docs/specs/2026-09-28-oracle-aidp-connectors-design.md` (scope,
   non-goals, the unverified-API questions the spike must answer, definition
   of done) and draft `connectors/<source>/{REQUIREMENTS,CLAUDE}.md`
   (template: `connectors/jira/`). **Get explicit user approval before
   writing any plan or code** — root `CLAUDE.md`'s process rule.
3. **Plan written.** Invoke `superpowers:writing-plans`, shaped like
   `docs/superpowers/plans/2026-09-29-jira-cloud-connector.md`:
   scaffold/session → **spike (hard gate)** → query/request builder → HTTP
   layer with bounded retry → paging → typed Spark DataFrame →
   docs/README/notebook → live-AIDP-test handoff guide. **No skill file or
   `.claude-plugin/` entries** — this repo removed that wrapper deliberately
   (root `CLAUDE.md`); don't restore it without the user raising it. Get
   plan approval before implementing.
4. **Spike is a real hard gate.** Probe identity/auth first (cheapest call,
   confirms auth before anything is built on it), then the main resource,
   against a real free/self-service endpoint. Record verified facts in
   `spike/RESULTS.md` with an explicit **Proceed**/**Stop** line — don't
   write the query-builder task or later until it says Proceed. One test
   request before any bulk/loop operation while live-debugging auth — a
   wrong credential retried in a loop can trip a lockout.
5. **Live AIDP run is out of Claude's reach.** The plan's final task is a
   `LIVE_TEST_GUIDE.md` handoff (template: `connectors/jira/LIVE_TEST_GUIDE.md`),
   never a claimed PASS. Only mark `live-results/RESULTS.md` PASS once it
   actually ran on a real cluster.

## Reuse before extending

Check `connectors/_shared/` first — `aidp_http.py` (retry/backoff),
`aidp_secrets.py` (credentials), `aidp_jars.py` (JDBC/jar loading). Most REST
connectors need nothing new here. Extract new shared code only when a
second connector actually needs it.

## Definition of done

Implemented; unit-tested offline (no network, no Spark — mock the
transport); executed against a real endpoint from a live AIDP workspace;
recorded with a dated PASS in `live-results/`; gotchas documented, each
claim labeled **verified** (observed live, dated) or **unverified**. No live
PASS = experimental, not listed as supported in the root README.

## Common mistakes

- Paging/query code before the spike confirms the real API's paging — Jira's
  reason for the gate: its old offset endpoint was silently sunset.
- Hitting the main resource before the identity/auth probe.
- Re-adding `skills/` or `.claude-plugin/` because Jira's git history has
  them — ask the user first.
- Claiming a live PASS from local/offline testing — a local Spark+Delta
  rehearsal (`TESTING.md`) isn't a live AIDP run.
- Committing a real hostname, email, or token anywhere, including a
  notebook cell's saved output.

## References

- Example: `connectors/jira/` and its plan,
  `docs/superpowers/plans/2026-09-29-jira-cloud-connector.md`
- Design: `docs/specs/2026-09-28-oracle-aidp-connectors-design.md`
- Candidates: GitHub issue #1 · Project rules: root `CLAUDE.md`
