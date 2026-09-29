# Testing

## Offline unit tests

```
pip install -r requirements-dev.txt
pytest -q
```

Runs from the repo root (matches CI, `.github/workflows/tests.yml`). No
network, no Spark, no live credentials required.

## Live testing a connector

Each connector's live test needs its own free account/site — see that
connector's `CLAUDE.md` for setup. General rules:

- Put credentials in a local `.env` at the repo root (already gitignored).
  Load it with a small Python parser, not shell `source`/`export` — values
  containing `=` or `$` are mishandled by naive shell sourcing.
- Never commit a real hostname, instance name, email, or token. Use the
  placeholder shown in that connector's example notebook and tests.
- Run the connector's example notebook on a live AIDP cluster, check the
  acceptance criteria in its `REQUIREMENTS.md`, and record the result in
  `connectors/<source>/live-results/RESULTS.md` — PASS only if it actually
  ran; otherwise NOT RUN with the reason.
- Free-tier accounts (developer instances, personal sites) can hibernate, be
  reclaimed, or rate-limit. Document what you hit.
