# Changelog

## Unreleased

- Jira Cloud connector: implemented, unit-tested, spike-verified live against
  a real site. Live AIDP run (Task 8) still pending.
- Extracted `connectors/_shared/` (`aidp_http`, `aidp_secrets`, `aidp_jars`),
  following Oracle's own connectors-plugin pattern of one shared package
  uploaded once per workspace. `aidp_secrets` adds OCI Vault credential
  resolution; `aidp_jars` (adapted from Oracle, MIT, not yet used by a shipped
  connector) adds SHA-256-verified jar downloads for the next jar-based
  connector. 85 tests total.
- Fixed: a whitespace-only JQL `query` produced invalid `()`.
