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

## Local Spark/Delta validation (no AIDP needed)

Before an AIDP cluster is available, a connector's write and incremental-merge
logic can be proven against a real local Spark + Delta session — a stronger
check than offline unit tests (which mock Spark entirely), though not a
substitute for a live AIDP run (the credential store, cluster networking, and
AIDP's exact runtime versions are still unverified until that happens).

```
python3.12 -m venv .venv-spark   # PySpark 3.5.x needs Python <=3.12, not 3.14
. .venv-spark/bin/activate
pip install pyspark==3.5.1 delta-spark==3.1.0 requests
python connectors/jira/spike/local_spark_check.py
```

Needs Java 8, 11, or 17 on `PATH` (`java -version`) and a `.env` with that
connector's live credentials. It runs the same calls the example notebook
makes — read, `to_dataframe`, a Delta write, then a second incremental run —
against a scratch local Delta table
(`connectors/jira/spike/out/warehouse/`, gitignored) and asserts the row
count never diverges from the distinct-key count across runs.
