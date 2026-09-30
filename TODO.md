# TODO

Working list, 2026-09-30. Baseline for "what's already covered" is upstream
`oracle-samples/oracle-aidp-samples` (commit `90b42d6`): its
`ai/claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors`
plugin (27 skills) plus the notebooks under `data-engineering/ingestion/`.

## 1. Unblock Zendesk

- [ ] Decide: test against a Zendesk account created before 2026-07-28 (API
      tokens still allowed), or redesign the connector for OAuth. See
      `connectors/zendesk/CLAUDE.md` — **verified** finding, 2026-09-30.
- [ ] Run the live spike, then Tasks 3+ of the Zendesk plan.

## 2. Next connectors (ranked against the upstream baseline)

Upstream covers Oracle/OCI sources, the common RDBMSs, Hive, Snowflake,
ADLS, S3, Excel, custom JDBC, and only two SaaS sources (Salesforce,
NetSuite). Its generic REST connector needs an AIDP-format manifest, which
real SaaS APIs don't publish — so SaaS APIs remain this repo's niche.

| # | Candidate | Gap it fills upstream | Technique new to this repo | Auth friction |
|---|---|---|---|---|
| 1 | MongoDB Atlas | No NoSQL at all | Spark connector jar via `_shared/aidp_jars.py` (unused so far); nested docs, schema drift, ObjectId | Low — free tier |
| 2 | GitHub | No dev/engineering SaaS | Link-header pagination, secondary rate limits | Very low — PAT |
| 3 | BigQuery | Snowflake only, no Google warehouse | Service-account JSON auth + connector jar | Low-medium |
| 4 | HubSpot | Only Salesforce for CRM | CRM counterpart to upstream Salesforce | Very low |
| 5 | Stripe (test mode) | No payments/billing | Events-API incremental pattern | Very low |
| 6 | Google Sheets | No business-user sources | Service-account auth (after BigQuery) | Low |

Rejected for now:

- **Confluence** — cheap (reuses Jira auth) but teaches nothing new.
- **SFTP** — needs `paramiko`; upstream notes some clusters can't reach PyPI.
- **Private-network sources, webhooks** — out of scope per design non-goals.

Facts from upstream's `tests/live-results/RESULTS.md` (Oracle's findings,
**unverified** by this repo):

- The cluster reaches the public internet and Maven Central via NAT egress,
  so runtime jar loading works (upstream S3 and Postgres rows PASS).
- The cluster pod network can't reach customer VCNs without admin-set
  peering; upstream MySQL/SQL Server rows stayed NOT RUN for this reason.
- `aidataplatform` `type=POSTGRESQL` ignores SSL options; SSL targets need
  native Spark JDBC with `sslmode=require` in the URL.

## 3. Upstream contributions to oracle-samples/oracle-aidp-samples

Upstream accepts outside PRs (`CONTRIBUTING.md`; several merged PRs from
non-`@oracle.com` authors).

Prerequisites:

- [ ] Oracle Contributor Agreement — likely the **corporate** OCA signed by
      Arbisoft (oca.opensource.oracle.com), since this is Arbisoft work.
      Confirm with Arbisoft.
- [ ] Choose this repo's licence (upstream is UPL; README open item).
- [ ] Every commit upstream needs `Signed-off-by:` (`git commit -s`).

First PR: Jira as a notebook sample.

- [ ] Open an upstream issue first (required by their process).
- [ ] Add `jira.py` plus an example notebook under `data-engineering/ingestion/`,
      and a row in the README Sample Catalog.
- [ ] Use upstream's own auth/jar helpers rather than duplicating `_shared/`.
- [ ] Nothing gets posted upstream without an explicit go-ahead.

Later, optionally: a plugin skill (`skills/aidp-jira/SKILL.md`, helper under
`scripts/oracle_ai_data_platform_connectors/rest/`, a live-results row, and
the Codex plugin copy, following upstream PR #94).

Side option: upstream left ADLS, MySQL, SQL Server and generic REST as
NOT RUN. A live PASS on any of these could be contributed back.
