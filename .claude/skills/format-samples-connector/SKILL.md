---
name: format-samples-connector
description: Use when turning a connector from this repo into an oracle-aidp-samples contribution, or bringing an existing samples PR into line with the samples repo's conventions — "format X for the samples repo", "make the PR match the existing connectors", "convert to a single notebook". Gives the folder layout, notebook template, catalog row, dependency and credential conventions, the port-back step, and a checker script.
---

# format-samples-connector

## Overview

`oracle-aidp-samples` (Oracle's repo, mirrored at `arbisoft/oracle-aidp-samples`)
has two kinds of ingestion sample:

- **Built-in connector notebooks** — `Read_Only_Ingestion_Connectors/`,
  `Read_Write_*_Connectors/`: one notebook per AIDP built-in type
  (`format("aidataplatform").option("type", ...)`), shipped by Oracle with
  product releases ("Add 4.0/4.1 connector samples").
- **Pattern samples** — `Connect_Using_Custom_JDBC_Driver.ipynb`,
  `Ingest_from_Multi_Cloud.ipynb`, `Read_excel_data/`: integrations that need
  something installed on the cluster (jars, Python packages).

Our connectors are not built-in AIDP types (REST code, third-party Spark
connectors), so they are contributed **as pattern samples, written in the
built-in connector notebooks' cell style.** Putting them among Oracle's
built-in connectors would imply Oracle supports them.

This repo (`oracle-aidp-connectors`) stays where connector logic is developed
and unit-tested (root `CLAUDE.md`). The samples contribution is one notebook
holding that logic in cells, plus at most a short README and a
`requirements.txt`.

**Core principle:** match Oracle's samples on `main`, not other open PRs. A
helper module with tests, or a package driven by a YAML config, diverges from
the samples repo even if it works.

## Step 0 — port back first (hard gate)

A samples PR may hold the only copy of code that passed on AIDP (fixes found
during a live run land where the run happened). **Before deleting a helper
module or tests from a samples PR, make sure this repo's
`connectors/<source>/` has the same logic and tests, and its
`live-results/RESULTS.md` says which version passed.** Diff them; port and
run the tests here first. Never let the conversion be the moment verified
code disappears.

## Reference samples (read before writing)

In `data-engineering/ingestion/` on `main`:

| For | Read |
|---|---|
| Cell style and the options table | `Read_Only_Ingestion_Connectors/Fusion_BICC.ipynb` (smallest), `Kafka.ipynb`, `REST.ipynb`; `Read_Write_External_Ecosystem_Connectors/PostgreSQL.ipynb` |
| Folder with README + requirements | `Read_excel_data/` |
| Jars installed on the cluster | `Connect_Using_Custom_JDBC_Driver.ipynb`, `Ingest_from_Multi_Cloud.ipynb` |
| Python package on the cluster | `Ingest_from_Multi_Cloud.ipynb` (boto3 section), `data-engineering/adw-iceberg-external-table-sync/` |
| Contribution rules | `CONTRIBUTING.md` (catalog row, PR process, sign-off) |

## Layout

```
data-engineering/ingestion/<Source>/
  <Source>.ipynb       required
  README.md            optional, short: what it does, prerequisites, limits
  requirements.txt     only if Prerequisites install it as a cluster library
```

`<Source>` in `Title_Snake_Case`, the same for folder and notebook
(`Jira_Cloud/Jira_Cloud.ipynb`, `MongoDB_Atlas/MongoDB_Atlas.ipynb`). No
helper modules, `tests/`, `pytest.ini` or `requirements-dev.txt` — those live
in this repo.

## Notebook template

Cells, in order:

1. **Code** cell (not markdown, not raw), exactly:
   ```
   Oracle AI Data Platform v1.0

   Copyright © 2025, Oracle and/or its affiliates.

   Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
   ```
2. **Markdown** title and one or two sentences:
   ```
   # <Source> Connector Samples

   Read-only ingestion samples for <Source> using <the library or Spark format>. Replace all placeholders before running a sample.
   ```
3. **`## Prerequisites`** markdown, numbered: cluster libraries, the
   Credential Store entry, network access (e.g. an IP allow-list).
4. One section per scenario: a **`## <Scenario>`** markdown cell with one
   explanatory sentence, then **one code cell**. Typical: `## Configuration`,
   `## Ingestion Sample`, `## Write to Delta`, `## Incremental Load`. `###`
   sub-headings are fine under a `##` section (REST does this).
5. Last: **`## Connector Options`** markdown table, exactly these columns:
   ```
   | Parameter name | Valid values | Mandatory | Description |
   | --- | --- | --- | --- |
   ```
   For a third-party Spark connector: its real option names
   (`connection.uri`, `database`, `collection`, …). For a Python-based
   connector: the notebook's configuration variables.

**Code style** (from the built-in notebooks):
- Backslash continuation, one `.option(...)` per line, end a read with `.show()`.
- DataFrame names `<source>_df`, `<source>_df_pushdown`.
- Placeholders are `<UPPER_SNAKE_CASE>`. Never a real host, user, catalog,
  OCID, IP or token — not in a comment, not in a saved output.
- Keep everything the source needs to work correctly (paging, 429 retry,
  timezone handling, a watermark): it moves into cells, compacted, not
  dropped. Drop only scaffolding the notebook has no use for (CLI wrappers,
  config-file loaders, abstraction layers). A function defined in one cell
  and used in a later one is fine when the logic needs it; say what it does
  in the markdown above.

**Metadata:** `nbformat` 4 / `nbformat_minor` 5; kernelspec
`{"display_name": "Python 3 (ipykernel)", "language": "python", "name": "python3"}`.
Every code cell `"execution_count": null, "outputs": []`.

## Catalog row

Per `CONTRIBUTING.md`: one row in the root `README.md` table **Data
Engineering — Ingestion**, linking the notebook:
```
| [<Display Name>](data-engineering/ingestion/<Source>/<Source>.ipynb) | <One sentence starting with an action verb.> |
```
Keep the table's alphabetical order.

## Dependencies

- **Jars:** Prerequisites list each jar with its Maven Central link and
  SHA-256, then: upload to a workspace folder through the UI → cluster
  **Library** tab → **Install Library** → **Workspace** → select →
  **Install**, one at a time → **Actions → Restart**.
- **Python packages:** a `requirements.txt` in the sample folder, installed
  as a cluster library from the same tab, then restart. Not `%pip install` in
  the notebook — it does not survive a scheduled job
  (`adw-iceberg-external-table-sync/README.md`).
- Packages already on the cluster need no prerequisite (`requests`).
- **No runtime jar loading** (`SparkContext.addJar`, class-loader tricks).

Verified on AIDP 2026-10-01 (MongoDB live run):
- `addJar` is not enough for a Spark DataSource: the driver loaded it, every
  executor task failed with `UnknownReason`.
- Jars written by the driver to `/Workspace/...` did not show in the
  **Install Library** file picker; jars uploaded through the UI did.
- Only one library change runs at a time per cluster ("ongoing operation").

## Credentials

Read secrets from the **AIDP Credential Store**, with a placeholder name:
```python
<SECRET> = aidputils.secrets.get(name="<CREDENTIAL_NAME>", key="<KEY>")
```
Prerequisites: "Create a **Secret Token** credential in the Credential Store
with key `<KEY>`." Verified on AIDP 2026-10-01: `aidputils.secrets.get(name,
key=None)` returns the whole map without `key`; notebooks cannot prompt for
input (`getpass` raises `StdinNotImplementedError`); the cluster UI had no
environment-variable setting. No OCI Vault / `oci` SDK path.

## Procedure

1. **Step 0** — port back (above).
2. Read the reference samples.
3. Pick the scenarios from what the connector does.
4. Write the notebook from the template; move the connector's logic into the
   cells (keep what the source needs, see *Code style*).
5. Verified gotchas become one-line notes in the section that needs them or
   in Prerequisites; the full reasoning stays in this repo's docs.
6. Optional `README.md` (short) and `requirements.txt`; delete everything
   else from the samples folder.
7. Catalog row.
8. Run the checker until it passes.
9. **Run the notebook on AIDP.** A live PASS of an earlier layout does not
   cover the rewritten notebook (root `CLAUDE.md`: never mark PASS unless it
   ran). Record it in this repo's `live-results/`.
10. PR per `CONTRIBUTING.md`: reference an issue, explain how to validate.
    Internal PRs against `arbisoft:main` are not signed off; a PR upstream to
    Oracle needs `Signed-off-by` (OCA) on every commit.

## Checker

```bash
python3 .claude/skills/format-samples-connector/check_sample_notebook.py \
  ../oracle-aidp-samples/data-engineering/ingestion/<Source>/<Source>.ipynb
```
Checks the folder layout, the UPL code cell, title, section headings, the
options table, metadata, empty outputs and placeholder-only credentials. It
also accepts Oracle's built-in layout, and passes 17 of the 19 built-in
connector notebooks on `main` (2026-10-01); the other two are Oracle's own
slips — `DB2.ipynb` (markdown UPL cell) and `Autonomous_AI_Lakehouse.ipynb`
(`"PASSWORD"` without angle brackets) — don't copy them.

## Common mistakes

- Deleting a samples PR's helper/tests before this repo has them (Step 0).
- Placing a non-built-in connector in `Read_Only_Ingestion_Connectors/`.
- Dropping source-required logic (retry, paging) to make cells shorter.
- Markdown or raw UPL cell instead of a **code** cell; © 2026 instead of 2025.
- Keeping `tests/` or a helper module in the samples folder.
- Literal secrets, real catalog/host names, or saved outputs left from a live run.
- Claiming the rewritten notebook is verified because an earlier layout passed.
