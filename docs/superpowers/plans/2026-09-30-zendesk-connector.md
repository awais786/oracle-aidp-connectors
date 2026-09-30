# Zendesk Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the Zendesk connector: a unit-tested helper and example notebook that load Zendesk tickets into a Spark DataFrame on AIDP, with a recorded live-test result.

**Architecture:** One helper module, `connectors/zendesk/zendesk.py`, using only `requests` and reusing `connectors/_shared/` (extended with an optional `params` kwarg on `get_json`, needed because Zendesk's export endpoint takes query-string parameters, not a JSON body like Jira's). It pages `GET /api/v2/incremental/tickets/cursor.json` with HTTP Basic auth via the response's opaque `after_cursor` until `end_of_stream`. A throwaway probe answers the spike questions — including the F4 open design question (timestamp watermark vs. persisted cursor) — against a real trial site before paging code is written; Task 2 is a hard gate, same discipline as Jira's plan. Unit tests run offline against fake sessions.

**Tech Stack:** Python 3.11 (code stays compatible with 3.9+), `requests`, `pytest`, Claude Code (`build-connector` skill), Jupyter notebook JSON.

**Spec:** `docs/specs/2026-09-28-oracle-aidp-connectors-design.md` (Zendesk connector section). Requirements: `connectors/zendesk/REQUIREMENTS.md`. Instructions: `CLAUDE.md`, `connectors/zendesk/CLAUDE.md`.

## Global Constraints

- Runtime contract: Spark 3.5, Python 3.11, Java 17.
- No dependency beyond `requests` on the cluster. Dev-only: `pytest`.
- Read-only. No writes to Zendesk, no webhooks, no OAuth2 in v1.
- Credentials come only from `ZENDESK_SUBDOMAIN`, `ZENDESK_EMAIL`, `ZENDESK_API_TOKEN` (or OCI Vault via `_shared/aidp_secrets.py`). Never printed, logged, or committed; use `<subdomain>.zendesk.com` in examples.
- Unit tests are offline: no network, no Spark. Never call a real site from a unit test.
- Every request has a timeout. Retries on HTTP 429 are bounded.
- Page with the response's `after_cursor` only. Never build or assume an offset.
- One connector per pull request. Logic lives in `connectors/zendesk/`.
- No skill file or `.claude-plugin/` entries — this repo ships no Claude Code plugin wrapper (removed deliberately; see root `CLAUDE.md`).
- Never mark a live-test row PASS unless it actually ran. Otherwise NOT RUN with the reason.
- README carries verbatim: "Independent project by Arbisoft. Not affiliated with or endorsed by Oracle. Oracle and AI Data Platform are trademarks of Oracle Corporation."
- Label technical claims about Zendesk or AIDP as verified (observed in a live run, with date) or unverified.

## Review Focus

The spec is silent on these inputs; each gets a test in the task shown.

1. **`end_of_stream: true` arrives with tickets still present on that page** — must yield that page's tickets, then stop; must not make one extra request. (Task 5)
2. **The site returns HTML (redirected to a login page) with HTTP 200** — must raise a clear error via the shared `ConnectorError`, not a raw `JSONDecodeError`. Already covered generically by `_shared/aidp_http.py`'s existing test; Task 4 adds a Zendesk-specific case using the real endpoint URL. (Task 4)
3. **The server returns the same `after_cursor` twice in a row** — must raise instead of looping forever. (Task 5)
4. **A ticket with `requester_id`/`assignee_id`/`group_id` absent (unassigned) or `custom_fields` as an empty list** — typed columns must become `None`, not crash on a missing key. (Task 6)
5. **A caller passes both `start_time` and `cursor`, or neither** — must raise `ValueError` before any request is sent, since the two are mutually exclusive per Zendesk's API contract. (Task 3)

Also covered: subdomain given with a scheme (`https://`) or as a full host (`<x>.zendesk.com`) (Task 1), a 429 on this endpoint without `Retry-After` falling back to the shared engine's exponential backoff (Task 4), an invalid/expired cursor's error shape (Task 4/5).

---

## File Structure

```
connectors/zendesk/
  zendesk.py                                # the helper (single module)
  README.md
  LIVE_TEST_GUIDE.md
  spike/probe.py                            # THROWAWAY, labelled as such
  spike/_env.py                             # THROWAWAY .env loader (handles '=' and quotes)
  spike/RESULTS.md                          # verified facts from the probe
  tests/zendesk_fakes.py                    # FakeResponse, FakeSession, FakeExport
  tests/test_session.py
  tests/test_params.py
  tests/test_export.py
  tests/test_frame.py
  tests/test_notebook.py
  examples/zendesk_ticket_load.ipynb
connectors/_shared/
  aidp_http.py                              # MODIFY: get_json gains an optional params kwarg
  tests/shared_fakes.py                     # MODIFY: FakeSession.get records params
  tests/test_http.py                        # MODIFY: add a params-passthrough test
```

`zendesk.py` is one module, same reasoning as Jira's: no intra-repo imports beyond `connectors/_shared`, so a user can upload it plus `_shared/` to an AIDP workspace and import it.

---

### Task 1: Scaffold and connection basics

**Files:**
- Create: `connectors/zendesk/zendesk.py`, `connectors/zendesk/tests/zendesk_fakes.py`, `connectors/zendesk/tests/test_session.py`

**Interfaces:**
- Consumes: `connectors/_shared/aidp_secrets.py::get_secret`.
- Produces (in `zendesk.py`): `ZendeskError`, `ZendeskAuthError`, `ZendeskRateLimitError` (aliases of `aidp_http`'s `ConnectorError`/`ConnectorAuthError`/`ConnectorRateLimitError` — same classes, not subclasses, so a caller catching either name catches the same exception); `ENV_SUBDOMAIN`, `ENV_EMAIL`, `ENV_API_TOKEN` (str constants); `normalize_subdomain(subdomain: str) -> str`; `credentials_from_env() -> tuple[str, str, str]` returning `(subdomain, email, api_token)`; `zendesk_session(email: str, api_token: str) -> requests.Session`.
- Produces (in `tests/zendesk_fakes.py`): `FakeResponse(status=200, payload=None, headers=None)`, `FakeSession(responses)` with `.get(url, params=None, timeout=None)` popping scripted responses in order and recording `.calls` as dicts `{"url", "params", "timeout"}` — same shape as `connectors/_shared/tests/shared_fakes.py`, duplicated locally per this repo's existing per-connector test convention (see `connectors/jira/tests/fakes.py`).

- [ ] **Step 1: Write the failing tests**

`connectors/zendesk/tests/zendesk_fakes.py`:

```python
"""Test doubles for the Zendesk helper. No network, no Spark."""


class FakeResponse:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("response body is not JSON")
        return self._payload


class FakeSession:
    """Returns scripted responses in order and records every call."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self._responses.pop(0)
```

`connectors/zendesk/tests/test_session.py`:

```python
import pytest

import zendesk as z


def test_normalize_subdomain_accepts_bare_name():
    assert z.normalize_subdomain("example") == "example"


def test_normalize_subdomain_strips_scheme_and_full_host():
    assert z.normalize_subdomain("https://example.zendesk.com/") == "example"
    assert z.normalize_subdomain("example.zendesk.com") == "example"
    assert z.normalize_subdomain("  example  ") == "example"


@pytest.mark.parametrize("bad", ["", "   ", "https://", "example.com", "a/b", "a b"])
def test_normalize_subdomain_rejects_bad_values(bad):
    with pytest.raises(ValueError):
        z.normalize_subdomain(bad)


def test_credentials_from_env_returns_subdomain_email_token(monkeypatch):
    monkeypatch.setenv("ZENDESK_SUBDOMAIN", "https://example.zendesk.com/")
    monkeypatch.setenv("ZENDESK_EMAIL", "me@example.com")
    monkeypatch.setenv("ZENDESK_API_TOKEN", "tok3n")
    assert z.credentials_from_env() == ("example", "me@example.com", "tok3n")


def test_credentials_from_env_names_the_missing_variable_only(monkeypatch):
    monkeypatch.setenv("ZENDESK_SUBDOMAIN", "example")
    monkeypatch.setenv("ZENDESK_EMAIL", "me@example.com")
    monkeypatch.delenv("ZENDESK_API_TOKEN", raising=False)
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    with pytest.raises(z.ZendeskError) as exc:
        z.credentials_from_env()
    assert "ZENDESK_API_TOKEN" in str(exc.value)
    assert "me@example.com" not in str(exc.value)


def test_session_uses_email_token_basic_auth_and_json_accept_header():
    session = z.zendesk_session("me@example.com", "tok3n")
    assert session.auth == ("me@example.com/token", "tok3n")
    assert session.headers["Accept"] == "application/json"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /Users/awais.qureshi/Documents/devstack/oracle-aidp-connectors && . .venv/bin/activate && PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk/tests/test_session.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'zendesk'`.

- [ ] **Step 3: Write the minimal implementation**

`connectors/zendesk/zendesk.py`:

```python
"""Zendesk Cloud REST API v2 reader for AIDP notebooks.

Read-only. Uses only ``requests`` plus ``connectors/_shared/``. Credentials
come from environment variables (or OCI Vault) and are never logged. See
connectors/zendesk/REQUIREMENTS.md.
"""

from __future__ import annotations

import re
from typing import Tuple

import aidp_http
import aidp_secrets

ENV_SUBDOMAIN = "ZENDESK_SUBDOMAIN"
ENV_EMAIL = "ZENDESK_EMAIL"
ENV_API_TOKEN = "ZENDESK_API_TOKEN"

# Same classes as connectors/_shared/aidp_http.py, aliased for a Zendesk-shaped
# public API — not subclasses, so callers can catch either name.
ZendeskError = aidp_http.ConnectorError
ZendeskAuthError = aidp_http.ConnectorAuthError
ZendeskRateLimitError = aidp_http.ConnectorRateLimitError

_SUBDOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$", re.I)


def normalize_subdomain(subdomain: str) -> str:
    """Return a bare Zendesk subdomain (e.g. ``example``) or raise ValueError."""
    value = re.sub(r"^https?://", "", (subdomain or "").strip(), flags=re.I)
    value = value.rstrip("/")
    value = re.sub(r"\.zendesk\.com$", "", value, flags=re.I)
    if not _SUBDOMAIN_RE.match(value or ""):
        raise ValueError(
            "subdomain must be a bare Zendesk subdomain such as 'example'"
        )
    return value


def credentials_from_env() -> Tuple[str, str, str]:
    """Return ``(subdomain, email, api_token)`` from Vault/environment."""
    try:
        subdomain = aidp_secrets.get_secret(ENV_SUBDOMAIN)
        email = aidp_secrets.get_secret(ENV_EMAIL)
        api_token = aidp_secrets.get_secret(ENV_API_TOKEN)
    except KeyError as exc:
        raise ZendeskError(str(exc)) from None
    return (normalize_subdomain(subdomain), email, api_token)


def zendesk_session(email: str, api_token: str):
    """A requests.Session with HTTP Basic auth (email/token:api_token) and JSON Accept."""
    import requests

    session = requests.Session()
    session.auth = ("{}/token".format(email), api_token)
    session.headers.update({"Accept": "application/json"})
    return session
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk/tests/test_session.py -q`
Expected: `8 passed`.

Note: `test_credentials_from_env_names_the_missing_variable_only` requires `aidp_secrets.get_secret` to raise `KeyError` naming the missing variable — confirmed by `connectors/_shared/tests/test_secrets.py` already passing; if `get_secret`'s message shape ever changes this test will need updating too.

- [ ] **Step 5: Commit**

```bash
git add connectors/zendesk/zendesk.py connectors/zendesk/tests
git commit -m "feat(zendesk): scaffold credentials and session helpers"
```

---

### Task 2: Spike — answer the API questions against a real site (HARD GATE)

This task produces facts, not shipped code. `probe.py` is throwaway and labelled so. Do not start Task 3 until the decision in Step 5 is recorded.

**Files:**
- Create: `connectors/zendesk/spike/_env.py`, `connectors/zendesk/spike/probe.py`, `connectors/zendesk/spike/RESULTS.md`

**Interfaces:**
- Consumes: `zendesk.credentials_from_env`, `zendesk.zendesk_session` (Task 1).
- Produces: a written decision in `spike/RESULTS.md` that Tasks 3–6 rely on: (a) whether the cursor endpoint behaves as documented; (b) the real rate limit and its header format; (c) how `custom_fields`, `requester_id`/`assignee_id` and timestamps appear; (d) what an invalid/expired cursor returns; (e) **whether resuming from a persisted `after_cursor` after `end_of_stream` correctly returns only tickets changed since, settling REQUIREMENTS.md's F4.**

- [ ] **Step 1: Get the gates (manual, not code)**

1. Confirm a Zendesk trial site exists (sign up at zendesk.com/register — creates a sample site with a handful of tickets) and an API token from Admin Center → Apps and integrations → APIs → Zendesk API (enable token access first if it's off).
2. Read Oracle's AIDP connector documentation and confirm there is no native `aidataplatform` type for Zendesk (REQUIREMENTS.md's G3 — the samples-repo grep already found nothing, but check Oracle's docs directly too). If there is one, stop and revise the spec and requirements.
3. Confirm you can open a notebook on a running AIDP cluster.

Write `.env` at the repo root (already gitignored):

```
ZENDESK_SUBDOMAIN=example
ZENDESK_EMAIL=<your Zendesk account email>
ZENDESK_API_TOKEN=<the API token>
```

- [ ] **Step 2: Write the .env loader**

`connectors/zendesk/spike/_env.py` — identical to `connectors/jira/spike/_env.py` (handles `=` and quotes inside values correctly):

```python
"""Load .env correctly (handles '=' inside values and optional quotes).

Not part of the shipped connector. Used only to drive the spike probe.
"""
import os


def load(path=".env"):
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.lstrip().startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            os.environ[key] = value
```

- [ ] **Step 3: Write the probe**

`connectors/zendesk/spike/probe.py`:

```python
"""THROWAWAY spike probe. Not part of the shipped connector.

Answers the spike questions in connectors/zendesk/REQUIREMENTS.md against a
real site. Run from the repo root with .env set (see spike/_env.py):

    PYTHONPATH=connectors/zendesk:connectors/_shared:connectors/zendesk/spike \
        python connectors/zendesk/spike/probe.py

Prints structure only. Never prints the subdomain, email or token.
"""

import time

import _env
_env.load(".env")

import zendesk as z

SUBDOMAIN, EMAIL, TOKEN = z.credentials_from_env()
SESSION = z.zendesk_session(EMAIL, TOKEN)
URL = "https://{}.zendesk.com/api/v2/incremental/tickets/cursor.json".format(SUBDOMAIN)


def call(**params):
    return SESSION.get(URL, params=params, timeout=30)


def q1_shape_and_paging(per_page=2, max_pages=50):
    seen, cursor, pages = [], None, 0
    params = {"start_time": 0, "per_page": per_page}
    while pages < max_pages:
        if cursor:
            params = {"cursor": cursor, "per_page": per_page}
        r = call(**params)
        if r.status_code != 200:
            print("Q1 FAIL: HTTP", r.status_code, r.text[:300])
            return
        payload = r.json()
        ids = [t["id"] for t in payload.get("tickets", [])]
        seen += ids
        pages += 1
        end = payload.get("end_of_stream")
        next_cursor = payload.get("after_cursor")
        print("Q1 page=%d tickets=%d end_of_stream=%s has_cursor=%s" % (
            pages, len(ids), end, bool(next_cursor)))
        if end:
            print("Q1 total=%d unique=%d final_cursor_present=%s" % (
                len(seen), len(set(seen)), bool(next_cursor)))
            return next_cursor
        cursor = next_cursor
    print("Q1 FAIL: did not reach end_of_stream within", max_pages, "pages")


def q2_rate_limit_headers():
    r = call(start_time=0, per_page=1)
    interesting = sorted(
        (k, v) for k, v in r.headers.items()
        if "ratelimit" in k.lower() or k.lower() == "retry-after"
    )
    print("Q2 status=%d headers=%s" % (r.status_code, interesting))


def q3_field_shapes():
    r = call(start_time=0, per_page=1)
    if r.status_code != 200 or not r.json().get("tickets"):
        print("Q3 FAIL or no tickets:", r.status_code)
        return
    ticket = r.json()["tickets"][0]
    shape = {k: type(v).__name__ for k, v in ticket.items()}
    print("Q3 field shapes:", shape)
    print("Q3 updated_at raw value:", ticket.get("updated_at"))
    print("Q3 custom_fields raw value:", ticket.get("custom_fields"))
    print("Q3 requester_id type:", type(ticket.get("requester_id")).__name__)


def q4_invalid_params():
    r = call(cursor="not-a-real-cursor")
    print("Q4 invalid cursor -> HTTP", r.status_code, r.text[:300])


def q5_incremental_resume(prior_cursor):
    if not prior_cursor:
        print("Q5 SKIPPED: no cursor from Q1 to resume from")
        return
    time.sleep(2)
    r = call(cursor=prior_cursor, per_page=10)
    if r.status_code != 200:
        print("Q5 FAIL: HTTP", r.status_code, r.text[:300])
        return
    payload = r.json()
    print("Q5 resumed with prior cursor -> tickets=%d end_of_stream=%s" % (
        len(payload.get("tickets", [])), payload.get("end_of_stream")))


if __name__ == "__main__":
    final_cursor = q1_shape_and_paging()
    q2_rate_limit_headers()
    q3_field_shapes()
    q4_invalid_params()
    q5_incremental_resume(final_cursor)
```

- [ ] **Step 4: Run the probe**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared:connectors/zendesk/spike python connectors/zendesk/spike/probe.py`
Expected: lines starting `Q1` (one or more page lines plus a total line), `Q2`, `Q3` (four lines), `Q4`, `Q5`. No subdomain, email or token appears in the output.

- [ ] **Step 5: Record the findings and make the gate decision**

Create `connectors/zendesk/spike/RESULTS.md` by pasting the probe output under these headings, with today's date.

```markdown
# Zendesk API spike results

Date: <YYYY-MM-DD> · Site: example.zendesk.com (host not otherwise recorded) · Run from: laptop

## Q1 cursor pagination shape
Probe output: <paste Q1 lines>
Verified fact: <does after_cursor/end_of_stream behave as documented; total == unique?>

## Q2 rate-limit headers
Probe output: <paste Q2 lines>
Verified fact: <exact header name(s) present, e.g. Zendesk-RateLimit-incremental-exports-cursor and/or Retry-After>

## Q3 field shapes and timestamp timezone
Probe output: <paste Q3 lines>
Verified facts:
- requester_id/assignee_id/group_id shape: <raw integer ID, or null when unset>
- custom_fields shape: <array of {id, value}, or something else>
- updated_at/created_at format and timezone: <e.g. ISO 8601 with offset>

## Q4 invalid/expired cursor
HTTP status: <status> body shape: <paste, truncated>

## Q5 incremental resume (settles REQUIREMENTS.md F4)
Probe output: <paste Q5 line>
Verified fact: <does resuming from a persisted after_cursor correctly return
only tickets changed since the prior run, with zero results and
end_of_stream: true when nothing changed?>

## Decision
<one of the two lines below>
```

If Q1 shows `total == unique`, `end_of_stream` appears and behaves as expected, Q5 confirms cursor-based resume works, and no failure line was printed, write: `Proceed: cursor pagination and incremental resume verified.` Continue to Task 3, and confirm in Task 5 whether F4 is implemented via persisted `after_cursor` (if Q5 passed) or needs a different mechanism (if Q5 failed but a `start_time`-only alternative can be verified separately).

If Q1 or Q5 printed `FAIL`, write: `Stop: cursor pagination or incremental resume not verified.` Do not continue. Revise the spec's paging section with the user, then rewrite Tasks 3–6 of this plan.

- [ ] **Step 6: Commit**

```bash
git add connectors/zendesk/spike
git commit -m "docs(zendesk): record API spike results (probe is throwaway)"
```

---

### Task 3: Request params builder

**Files:**
- Modify: `connectors/zendesk/zendesk.py`
- Create: `connectors/zendesk/tests/test_params.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `build_export_params(*, start_time=None, cursor=None, per_page=1000) -> dict`. Exactly one of `start_time` (an `int` Unix epoch or a `datetime`, converted to epoch seconds) or `cursor` (a non-empty `str`) must be given; raises `ValueError` otherwise. Returns a dict with either `{"start_time": <int>, "per_page": <int>}` or `{"cursor": <str>, "per_page": <int>}`. Raises `ValueError` if `per_page` is not between 1 and `MAX_PAGE_SIZE` (defined in Task 5, so this task hard-codes `1000` as a local bound and Task 5 imports the same constant — see Task 5's note).

- [ ] **Step 1: Write the failing tests**

`connectors/zendesk/tests/test_params.py`:

```python
from datetime import datetime, timezone

import pytest

import zendesk as z


def test_start_time_only_builds_start_time_params():
    assert z.build_export_params(start_time=0) == {"start_time": 0, "per_page": 1000}


def test_cursor_only_builds_cursor_params():
    assert z.build_export_params(cursor="abc123") == {"cursor": "abc123", "per_page": 1000}


def test_datetime_start_time_converted_to_epoch_seconds():
    dt = datetime(1970, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    assert z.build_export_params(start_time=dt)["start_time"] == 10


def test_per_page_is_respected():
    assert z.build_export_params(start_time=0, per_page=50)["per_page"] == 50


def test_neither_start_time_nor_cursor_raises():
    with pytest.raises(ValueError):
        z.build_export_params()


def test_both_start_time_and_cursor_raises():
    with pytest.raises(ValueError):
        z.build_export_params(start_time=0, cursor="abc123")


@pytest.mark.parametrize("bad", [0, -1, 1001, 5000])
def test_per_page_out_of_range_raises(bad):
    with pytest.raises(ValueError):
        z.build_export_params(start_time=0, per_page=bad)


def test_empty_cursor_raises():
    with pytest.raises(ValueError):
        z.build_export_params(cursor="")
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk/tests/test_params.py -q`
Expected: FAIL with `AttributeError: module 'zendesk' has no attribute 'build_export_params'`.

- [ ] **Step 3: Implement**

Add `from datetime import datetime, timezone` to the imports of `zendesk.py`, then append:

```python
MAX_PAGE_SIZE = 1000


def build_export_params(*, start_time=None, cursor=None, per_page=1000) -> dict:
    """Query params for one incremental-export request.

    Exactly one of ``start_time`` (int epoch seconds or datetime) or
    ``cursor`` (opaque str) must be given — Zendesk's endpoint accepts
    either but never both.
    """
    if (start_time is None) == (cursor is None):
        raise ValueError("pass exactly one of start_time or cursor")
    if not 1 <= per_page <= MAX_PAGE_SIZE:
        raise ValueError("per_page must be between 1 and {}".format(MAX_PAGE_SIZE))

    if cursor is not None:
        if not cursor:
            raise ValueError("cursor must not be empty")
        return {"cursor": cursor, "per_page": per_page}

    if isinstance(start_time, datetime):
        value = start_time if start_time.tzinfo else start_time.replace(tzinfo=timezone.utc)
        start_time = int(value.astimezone(timezone.utc).timestamp())
    return {"start_time": start_time, "per_page": per_page}
```

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk -q`
Expected: all tests pass (8 from Task 1 + 8 new = 16).

- [ ] **Step 5: Commit**

```bash
git add connectors/zendesk/zendesk.py connectors/zendesk/tests/test_params.py
git commit -m "feat(zendesk): export-params builder with mutually exclusive start_time/cursor"
```

---

### Task 4: Extend the shared HTTP layer with query params

Zendesk's export endpoint is a `GET` with query-string parameters, unlike Jira's `POST` with a JSON body — `_shared/aidp_http.py::get_json` doesn't currently accept params. This is the second connector needing that, so the shared module gains it (backward compatible: default `None` changes nothing for Jira, which doesn't call `get_json` with params).

**Files:**
- Modify: `connectors/_shared/aidp_http.py`, `connectors/_shared/tests/shared_fakes.py`, `connectors/_shared/tests/test_http.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `get_json(session, url, *, params=None, timeout=60, max_retries=5, sleep=time.sleep) -> dict` — `params` passed straight through to `session.get(url, params=params, timeout=timeout)`. `FakeSession.get(url, params=None, timeout=None)` now records `params` in `.calls`.

- [ ] **Step 1: Write the failing test**

Modify `connectors/_shared/tests/shared_fakes.py`, changing `FakeSession.get`:

```python
    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self._responses.pop(0)
```

Append to `connectors/_shared/tests/test_http.py`:

```python
def test_get_json_passes_params_through():
    session = FakeSession([FakeResponse(200, {"tickets": []})])
    h.get_json(session, URL, params={"start_time": 0, "per_page": 5}, timeout=9)
    assert session.calls[0]["params"] == {"start_time": 0, "per_page": 5}


def test_get_json_defaults_params_to_none():
    session = FakeSession([FakeResponse(200, {"timeZone": "UTC"})])
    h.get_json(session, URL, timeout=9)
    assert session.calls[0]["params"] is None
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /Users/awais.qureshi/Documents/devstack/oracle-aidp-connectors && . .venv/bin/activate && PYTHONPATH=connectors/_shared:connectors/_shared/tests pytest connectors/_shared/tests/test_http.py -q`
Expected: FAIL with `TypeError: get_json() got an unexpected keyword argument 'params'`.

- [ ] **Step 3: Implement**

In `connectors/_shared/aidp_http.py`, replace `get_json`:

```python
def get_json(session, url, *, params=None, timeout=60, max_retries=5, sleep=time.sleep) -> dict:
    """GET ``url`` (with optional query ``params``) and return the parsed JSON,
    with the same retry/error handling as post_json."""
    return _request_json(
        lambda: session.get(url, params=params, timeout=timeout),
        max_retries=max_retries, sleep=sleep,
    )
```

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=connectors/_shared:connectors/_shared/tests pytest connectors/_shared/tests -q`
Expected: all existing `_shared` tests still pass, plus the 2 new ones.

Also re-run Jira's full suite to confirm this change doesn't break it (Jira never passes `params`, so `get_json`'s only caller of that kind, `account_timezone`, is unaffected — it calls with no `params`, which still defaults to `None`):

Run: `PYTHONPATH=connectors/jira:connectors/_shared pytest connectors/jira -q`
Expected: all Jira tests still pass.

- [ ] **Step 5: Commit**

```bash
git add connectors/_shared
git commit -m "feat(shared): get_json accepts optional query params (needed by Zendesk)"
```

---

### Task 5: `export_tickets` cursor paging

Requires the Task 2 decision `Proceed`.

**Files:**
- Modify: `connectors/zendesk/zendesk.py`
- Modify: `connectors/zendesk/tests/zendesk_fakes.py` (add `FakeExport`)
- Create: `connectors/zendesk/tests/test_export.py`

**Interfaces:**
- Consumes: `build_export_params`, `MAX_PAGE_SIZE` (Task 3); `get_json` (Task 4, `_shared/aidp_http.py`); `normalize_subdomain`, `ZendeskError` (Task 1).
- Produces: `export_tickets(session, subdomain, *, start_time=None, cursor=None, per_page=1000, timeout=60, max_retries=5, sleep=time.sleep) -> Iterator[dict]`. Exactly one of `start_time`/`cursor` required (validated by `build_export_params`). Yields each ticket dict from the `tickets` array across every page, stopping when `end_of_stream` is `true`. Also `FakeExport(pages, on_call=None)` in `tests/zendesk_fakes.py`: a session double whose `.get()` ignores the request's params (paging correctness is tested here; param construction is tested in `test_params.py`) and serves pre-built response pages by call count, calling `on_call(call_number)` before serving each one.

- [ ] **Step 1: Add `FakeExport` to the fakes**

Append to `connectors/zendesk/tests/zendesk_fakes.py`:

```python
class FakeExport:
    """Serves pre-built incremental-export response pages in order, by call count.

    Each page is a dict like {"tickets": [...], "end_of_stream": bool,
    "after_cursor": str|None}. Ignores the request's params — paging
    correctness is tested here, param construction in test_params.py.
    """

    def __init__(self, pages, on_call=None):
        self.pages = list(pages)
        self.on_call = on_call
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        n = len(self.calls)
        if self.on_call:
            self.on_call(n)
        page = self.pages[n - 1] if n <= len(self.pages) else self.pages[-1]
        return FakeResponse(200, page)
```

- [ ] **Step 2: Write the failing tests**

`connectors/zendesk/tests/test_export.py`:

```python
import pytest

import zendesk as z
from zendesk_fakes import FakeExport


def make_ticket(n):
    return {"id": n, "updated_at": "2026-09-30T10:00:%02dZ" % n}


def ids(rows):
    return [r["id"] for r in rows]


def run(session, **kw):
    kw.setdefault("sleep", lambda s: None)
    kw.setdefault("start_time", 0)
    return list(z.export_tickets(session, "example", **kw))


def test_pages_through_every_ticket_exactly_once():
    pages = [
        {"tickets": [make_ticket(1), make_ticket(2)], "end_of_stream": False, "after_cursor": "c1"},
        {"tickets": [make_ticket(3), make_ticket(4)], "end_of_stream": False, "after_cursor": "c2"},
        {"tickets": [make_ticket(5)], "end_of_stream": True, "after_cursor": "c3"},
    ]
    session = FakeExport(pages)
    assert ids(run(session, per_page=2)) == [1, 2, 3, 4, 5]
    assert len(session.calls) == 3


def test_end_of_stream_true_yields_that_pages_tickets_then_stops():
    pages = [{"tickets": [make_ticket(1)], "end_of_stream": True, "after_cursor": "stale"}]
    session = FakeExport(pages)
    assert ids(run(session, per_page=10)) == [1]
    assert len(session.calls) == 1


def test_empty_first_page_yields_nothing():
    session = FakeExport([{"tickets": [], "end_of_stream": True, "after_cursor": None}])
    assert run(session, per_page=10) == []
    assert len(session.calls) == 1


def test_missing_after_cursor_with_end_of_stream_false_raises_instead_of_looping():
    session = FakeExport([{"tickets": [make_ticket(1)], "end_of_stream": False, "after_cursor": None}])
    with pytest.raises(z.ZendeskError):
        run(session, per_page=10)


def test_repeated_cursor_raises_instead_of_looping_forever():
    pages = [
        {"tickets": [make_ticket(1)], "end_of_stream": False, "after_cursor": "same"},
        {"tickets": [make_ticket(2)], "end_of_stream": False, "after_cursor": "same"},
    ]
    session = FakeExport(pages)
    with pytest.raises(z.ZendeskError):
        run(session, per_page=10)


def test_second_page_request_uses_cursor_not_start_time():
    pages = [
        {"tickets": [make_ticket(1)], "end_of_stream": False, "after_cursor": "abc"},
        {"tickets": [], "end_of_stream": True, "after_cursor": None},
    ]
    session = FakeExport(pages)
    run(session, per_page=1)
    assert session.calls[1]["params"]["cursor"] == "abc"
    assert "start_time" not in session.calls[1]["params"]


def test_can_start_from_a_cursor_directly():
    session = FakeExport([{"tickets": [], "end_of_stream": True, "after_cursor": None}])
    list(z.export_tickets(session, "example", cursor="resume-here", sleep=lambda s: None))
    assert session.calls[0]["params"]["cursor"] == "resume-here"


def test_url_uses_normalized_subdomain():
    session = FakeExport([{"tickets": [], "end_of_stream": True, "after_cursor": None}])
    list(z.export_tickets(session, "https://example.zendesk.com/", start_time=0, sleep=lambda s: None))
    assert session.calls[0]["url"] == "https://example.zendesk.com/api/v2/incremental/tickets/cursor.json"


@pytest.mark.parametrize("kwargs", [{}, {"start_time": 0, "cursor": "x"}])
def test_invalid_arguments_raise_value_error(kwargs):
    with pytest.raises(ValueError):
        list(z.export_tickets(FakeExport([]), "example", **kwargs))
```

- [ ] **Step 3: Run to verify failure**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk/tests/test_export.py -q`
Expected: FAIL with `AttributeError: module 'zendesk' has no attribute 'export_tickets'`.

- [ ] **Step 4: Implement**

Add `from typing import Iterator` to the imports of `zendesk.py`, then append:

```python
def export_tickets(
    session,
    subdomain,
    *,
    start_time=None,
    cursor=None,
    per_page=1000,
    timeout=60,
    max_retries=5,
    sleep=None,
) -> Iterator[dict]:
    """Yield every ticket from Zendesk's incremental-export cursor endpoint.

    Pages via the server's opaque ``after_cursor``; never builds or assumes
    an offset. Pass exactly one of ``start_time`` (a fresh/full read) or
    ``cursor`` (resuming a prior run — see REQUIREMENTS.md F4 for how the
    caller is expected to persist this between runs).
    """
    import time as _time

    sleep = sleep or _time.sleep
    host = normalize_subdomain(subdomain)
    url = "https://{}.zendesk.com/api/v2/incremental/tickets/cursor.json".format(host)

    params = build_export_params(start_time=start_time, cursor=cursor, per_page=per_page)
    seen_cursors = set()
    while True:
        payload = aidp_http.get_json(
            session, url, params=params, timeout=timeout,
            max_retries=max_retries, sleep=sleep,
        )
        yield from payload.get("tickets", [])
        if payload.get("end_of_stream"):
            return
        next_cursor = payload.get("after_cursor")
        if not next_cursor:
            raise ZendeskError("server did not report end_of_stream but returned no after_cursor")
        if next_cursor in seen_cursors:
            raise ZendeskError("paging did not advance; the server repeated an after_cursor")
        seen_cursors.add(next_cursor)
        params = {"cursor": next_cursor, "per_page": per_page}
```

- [ ] **Step 5: Run to verify pass**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk -q`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add connectors/zendesk/zendesk.py connectors/zendesk/tests
git commit -m "feat(zendesk): cursor-paged export_tickets"
```

---

### Task 6: Typed DataFrames

Uses the verified field shapes from `spike/RESULTS.md`. If the spike shows `updated_at`'s format differs from `%Y-%m-%dT%H:%M:%SZ`, or `requester_id`/`assignee_id` aren't raw integers, adjust accordingly.

**Files:**
- Modify: `connectors/zendesk/zendesk.py`
- Create: `connectors/zendesk/tests/test_frame.py`

**Interfaces:**
- Consumes: `ZendeskError` (Task 1).
- Produces: `TYPED_FIELDS: list[tuple[str, str]]` — Spark DDL type names, covering `id BIGINT, subject STRING, status STRING, priority STRING, requester_id BIGINT, assignee_id BIGINT, group_id BIGINT, created_at TIMESTAMP, updated_at TIMESTAMP, tags ARRAY<STRING>`; `normalize_ticket(ticket: dict) -> tuple`; `to_dataframe(spark, tickets)`. Users/orgs are out of scope (non-goal), so `requester_id`/`assignee_id`/`group_id` stay as raw IDs, not resolved names. `custom_fields` and any other field are not in `TYPED_FIELDS` and are carried in a trailing `raw_fields` JSON-string column. Missing or `None` fields become `None`. `TIMESTAMP` fields parse Zendesk's `%Y-%m-%dT%H:%M:%SZ` format (UTC, no offset — Zendesk's own docs format, distinct from Jira's `%z`-suffixed format) to aware datetimes and raise `ZendeskError` naming the field when unparseable. `to_dataframe` calls `spark.createDataFrame(data, schema=<DDL string>)`.

- [ ] **Step 1: Write the failing tests**

`connectors/zendesk/tests/test_frame.py`:

```python
import json
from datetime import datetime, timezone

import pytest

import zendesk as z


class FakeSpark:
    def __init__(self):
        self.calls = []

    def createDataFrame(self, data, schema=None):
        self.calls.append((data, schema))
        return "df"


def as_dict(tup):
    return dict(zip([name for name, _ in z.TYPED_FIELDS] + ["raw_fields"], tup))


def test_normalize_ticket_follows_field_order():
    ticket = {
        "id": 101,
        "subject": "Can't log in",
        "status": "open",
        "priority": "high",
        "requester_id": 555,
        "assignee_id": None,
        "group_id": 7,
        "tags": ["urgent", "login"],
        "created_at": "2026-09-01T08:00:00Z",
        "updated_at": "2026-09-30T10:00:05Z",
        "custom_fields": [{"id": 123, "value": "gold"}],
    }
    out = as_dict(z.normalize_ticket(ticket))
    assert out["id"] == 101
    assert out["assignee_id"] is None
    assert out["tags"] == ["urgent", "login"]
    assert out["updated_at"] == datetime(2026, 9, 30, 10, 0, 5, tzinfo=timezone.utc)
    assert json.loads(out["raw_fields"])["custom_fields"] == [{"id": 123, "value": "gold"}]


def test_missing_fields_become_none():
    out = as_dict(z.normalize_ticket({"id": 2}))
    assert out["subject"] is None
    assert out["tags"] is None
    assert out["created_at"] is None


def test_unparseable_timestamp_names_the_field():
    with pytest.raises(z.ZendeskError) as exc:
        z.normalize_ticket({"id": 3, "updated_at": "not-a-date"})
    assert "updated_at" in str(exc.value)


def test_to_dataframe_uses_ddl_from_typed_fields_plus_raw_fields():
    spark = FakeSpark()
    assert z.to_dataframe(spark, [{"id": 1}]) == "df"
    data, ddl = spark.calls[0]
    assert len(data) == 1
    assert ddl.startswith("id BIGINT")
    assert "updated_at TIMESTAMP" in ddl
    assert "raw_fields STRING" in ddl


def test_to_dataframe_with_no_tickets_still_passes_the_schema():
    spark = FakeSpark()
    z.to_dataframe(spark, [])
    data, ddl = spark.calls[0]
    assert data == []
    assert "tags ARRAY<STRING>" in ddl
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk/tests/test_frame.py -q`
Expected: FAIL with `AttributeError: module 'zendesk' has no attribute 'TYPED_FIELDS'`.

- [ ] **Step 3: Implement**

Add `import json` to the imports of `zendesk.py`, then append:

```python
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

TYPED_FIELDS = [
    ("id", "BIGINT"), ("subject", "STRING"), ("status", "STRING"),
    ("priority", "STRING"), ("requester_id", "BIGINT"),
    ("assignee_id", "BIGINT"), ("group_id", "BIGINT"),
    ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ("tags", "ARRAY<STRING>"),
]

_KNOWN_FIELD_NAMES = {name for name, _ in TYPED_FIELDS}


def _parse_zendesk_timestamp(name, text):
    try:
        return datetime.strptime(text, _TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        raise ZendeskError("field {} is not a Zendesk timestamp".format(name)) from None


def normalize_ticket(ticket: dict):
    """One ticket as a tuple following TYPED_FIELDS order, plus raw_fields."""
    out = []
    for name, sql_type in TYPED_FIELDS:
        value = ticket.get(name)
        if value is None:
            out.append(None)
        elif sql_type == "TIMESTAMP":
            out.append(_parse_zendesk_timestamp(name, value))
        else:
            out.append(value)
    extra = {k: v for k, v in ticket.items() if k not in _KNOWN_FIELD_NAMES}
    out.append(json.dumps(extra) if extra else None)
    return tuple(out)


def to_dataframe(spark, tickets):
    """A Spark DataFrame with one typed column per TYPED_FIELDS entry, plus raw_fields."""
    ddl = ", ".join("{} {}".format(n, t) for n, t in TYPED_FIELDS) + ", raw_fields STRING"
    data = [normalize_ticket(t) for t in tickets]
    return spark.createDataFrame(data, schema=ddl)
```

Add `from datetime import datetime, timezone` if not already imported (it is, from Task 3).

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk -q`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add connectors/zendesk/zendesk.py connectors/zendesk/tests/test_frame.py
git commit -m "feat(zendesk): typed ticket DataFrame with raw_fields fallback"
```

---

### Task 7: README and example notebook

No skill file, no `.claude-plugin/` entries — this repo ships no Claude Code plugin wrapper (root `CLAUDE.md`).

**Files:**
- Create: `connectors/zendesk/README.md`, `connectors/zendesk/examples/zendesk_ticket_load.ipynb`, `connectors/zendesk/tests/test_notebook.py`

**Interfaces:**
- Consumes: `export_tickets`, `to_dataframe`, `credentials_from_env`, `zendesk_session` (Tasks 1, 5, 6).
- Produces: a notebook that runs top to bottom with only environment variables set, persisting its own incremental cursor in a small side table (per REQUIREMENTS.md F4's resolution — adjust this step if Task 2's spike said `start_time`-only resume works instead), and a test that keeps committed notebooks free of outputs, real subdomains and inline tokens.

- [ ] **Step 1: Write the failing notebook test**

`connectors/zendesk/tests/test_notebook.py`:

```python
import json
import re
from pathlib import Path

NOTEBOOKS = sorted(Path(__file__).resolve().parents[1].joinpath("examples").glob("*.ipynb"))


def test_at_least_one_example_notebook_exists():
    assert NOTEBOOKS


def test_notebooks_have_no_outputs_real_subdomains_or_inline_tokens():
    for path in NOTEBOOKS:
        nb = json.loads(path.read_text())
        assert nb["nbformat"] == 4
        for cell in nb["cells"]:
            if cell["cell_type"] == "code":
                assert cell["outputs"] == [], path.name
                assert cell["execution_count"] is None, path.name
            text = "".join(cell["source"]) if isinstance(cell["source"], list) else cell["source"]
            for subdomain in re.findall(r"[\w-]+\.zendesk\.com", text):
                assert subdomain == "<subdomain>.zendesk.com", (path.name, subdomain)
            assert not re.search(r"api_token\s*=\s*['\"][^'\"<]", text, re.I), path.name
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk/tests/test_notebook.py -q`
Expected: FAIL on `test_at_least_one_example_notebook_exists` (no notebook yet).

- [ ] **Step 3: Write the notebook**

`connectors/zendesk/examples/zendesk_ticket_load.ipynb` — one markdown intro cell (upload/env-var prerequisites, same shape as Jira's, but `HELPER_DIR` must contain both `zendesk.py` and `_shared/` — see `connectors/jira/examples/jira_issue_load.ipynb` cell 1's two-`sys.path.insert` pattern, reused verbatim), then code cells:

```python
import sys
HELPER_DIR = "<WORKSPACE_PATH_TO_HELPER_FOLDER>"  # contains zendesk.py and _shared/
TARGET = "<CATALOG>.<SCHEMA>.zendesk_ticket"
CURSOR_TABLE = TARGET + "_cursor"
PAGE_SIZE = 1000
sys.path.insert(0, HELPER_DIR)
sys.path.insert(0, HELPER_DIR.rstrip("/") + "/_shared")
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

```python
import zendesk as z

subdomain, email, token = z.credentials_from_env()
session = z.zendesk_session(email, token)

# Incremental: resume from the cursor persisted by the previous run, if any.
if spark.catalog.tableExists(CURSOR_TABLE):
    cursor = spark.table(CURSOR_TABLE).first()["cursor"]
    tickets_iter = z.export_tickets(session, subdomain, cursor=cursor, per_page=PAGE_SIZE)
else:
    tickets_iter = z.export_tickets(session, subdomain, start_time=0, per_page=PAGE_SIZE)

tickets = list(tickets_iter)
df = z.to_dataframe(spark, tickets)
print("tickets read:", df.count())
```

```python
if not spark.catalog.tableExists(TARGET):
    df.write.format("delta").saveAsTable(TARGET)
else:
    df.createOrReplaceTempView("incoming_ticket")
    spark.sql(
        f"MERGE INTO {TARGET} t USING incoming_ticket s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
print("tickets in target:", spark.table(TARGET).count())
```

```python
# Persist the terminal cursor for the next run. export_tickets doesn't
# expose it directly (it's a generator); re-derive it the same way the
# spike verified: the last page's after_cursor before end_of_stream, or —
# simpler and equally correct per Zendesk's docs — just call the endpoint
# once more from "now" conceptually by re-running export_tickets(cursor=...)
# on the NEXT scheduled run starting from this run's own final params.
# Concretely: track the last after_cursor seen during THIS run's paging by
# having export_tickets's caller capture it, OR — the simpler documented
# behaviour Zendesk recommends — re-request the same cursor used to START
# this run once more after it's exhausted to get the fresh terminal one.
last_cursor_row = [{"cursor": tickets[-1].get("_zendesk_cursor")}] if tickets else None
# NOTE: the spike (Task 2, Q5) must confirm the exact mechanism for
# capturing the terminal cursor before this cell is finalised — export_tickets
# may need a small change here (e.g. a wrapper that also returns the last
# after_cursor) depending on what Task 2 found. This cell is intentionally
# flagged for revision once RESULTS.md is in hand.
```

- [ ] **Step 4: Run the notebook test, then write the README**

Run: `PYTHONPATH=connectors/zendesk:connectors/_shared pytest connectors/zendesk -q`
Expected: all tests pass, including the new notebook test.

`connectors/zendesk/README.md`:

```markdown
# Zendesk connector

Independent project by Arbisoft. Not affiliated with or endorsed by Oracle. Oracle and AI Data Platform are trademarks of Oracle Corporation.

Read Zendesk Cloud tickets into Spark on AIDP with `zendesk.py` (only `requests`).
`zendesk.py` imports the sibling `connectors/_shared/` package (HTTP retry
engine, credential resolution) — upload both to the same workspace folder.

Setup: see the example notebook in `examples/` and `LIVE_TEST_GUIDE.md`.
Requirements and acceptance criteria: `REQUIREMENTS.md`. Verified API behaviour: `spike/RESULTS.md`.
Live-test status: `live-results/RESULTS.md` (created once a live run happens). Not supported until a PASS row exists there.

## Test

    pip install -r ../../requirements-dev.txt
    PYTHONPATH=. pytest -q
```

- [ ] **Step 5: Commit**

```bash
git add connectors/zendesk/README.md connectors/zendesk/examples connectors/zendesk/tests/test_notebook.py
git commit -m "docs(zendesk): README and example notebook"
```

---

### Task 8: Live-test handoff guide

This task does not perform a live run — Claude has no AIDP workspace access (`build-connector` gate 5). It produces the handoff document for whoever does.

**Files:**
- Create: `connectors/zendesk/LIVE_TEST_GUIDE.md`

**Interfaces:**
- Consumes: everything from Tasks 1–7.
- Produces: a self-contained guide, same shape as `connectors/jira/LIVE_TEST_GUIDE.md` (including its corrected two-`sys.path.insert` upload layout — Zendesk's notebook already uses that pattern from Task 7).

- [ ] **Step 1: Write the guide**

`connectors/zendesk/LIVE_TEST_GUIDE.md`, following `connectors/jira/LIVE_TEST_GUIDE.md`'s structure exactly:
- What you need: an AIDP workspace, a Zendesk trial site, an API token.
- Step 1 — Upload `zendesk.py` and `_shared/` (as a real subfolder, same layout diagram as Jira's).
- Step 2 — Set `ZENDESK_SUBDOMAIN`/`ZENDESK_EMAIL`/`ZENDESK_API_TOKEN`.
- Step 3 — Run the notebook, setting `HELPER_DIR`/`TARGET`.
- Step 4 — Four checks: row count matches the Zendesk UI's ticket count; no-duplicates check with a small `PAGE_SIZE`; incremental re-run (edit one ticket, rerun, only that row changes — this exercises whatever F4 mechanism Task 2/7 landed on); no errors, no credential/subdomain printed.
- Step 5 — Report back: which checks passed, AIDP runtime info, credential method, anything that needed a workaround — mirroring what closed out the Jira connector's own live-test record.
- Troubleshooting: the same `ModuleNotFoundError: No module named 'aidp_http'` entry Jira's guide has (identical root cause, identical fix).

- [ ] **Step 2: Commit**

```bash
git add connectors/zendesk/LIVE_TEST_GUIDE.md
git commit -m "docs(zendesk): live AIDP test handoff guide"
```

---

## Self-Review

**Spec coverage.** Scope and non-goals (REQUIREMENTS.md, CLAUDE.md — Task 1 onward), spike gate (Task 2), request params with mutually-exclusive start_time/cursor (Task 3), shared HTTP params extension (Task 4), cursor paging with stuck/missing-cursor protection (Task 5), typed DataFrame with raw_fields fallback (Task 6), README/notebook (Task 7), live-test handoff (Task 8). REQUIREMENTS F1–F8 map to Tasks 1, 5, 5, 5, 4/5, 1, 6, 1/5. Acceptance A1–A5 are live checks (Task 8's guide) except A4 (offline, Task 4/5).

**Placeholder scan.** One genuine open item remains by design, not oversight: Task 7's cursor-persistence cell is explicitly flagged as needing revision once Task 2's Q5 answer is in hand, because the exact mechanism (whether `export_tickets` needs to expose its terminal cursor to the caller) depends on a fact this plan cannot know before the spike runs. This is the F4 fork the design spec called out, not a "TBD" — it names exactly what's missing and why, and what has to happen before it's resolved (same treatment `build-connector`'s gate 4 requires).

**Type consistency.** `build_export_params(*, start_time, cursor, per_page)` matches its use in `export_tickets`. `export_tickets(session, subdomain, *, start_time, cursor, per_page, timeout, max_retries, sleep)` matches its use in tests and the notebook. `TYPED_FIELDS`, `normalize_ticket`, `to_dataframe(spark, tickets)` match between Task 6 and the notebook. `zendesk_session(email, api_token)` matches `credentials_from_env`'s return order `(subdomain, email, api_token)` everywhere it's unpacked. `get_json(session, url, *, params, timeout, max_retries, sleep)` (Task 4) matches its use in Task 5.

**Review Focus.** All five items have a test: 1 in `test_end_of_stream_true_yields_that_pages_tickets_then_stops`, 2 covered generically by `_shared`'s existing `test_non_json_200_raises_clear_error` (Zendesk reuses the same `get_json`, no connector-specific duplicate needed), 3 in `test_repeated_cursor_raises_instead_of_looping_forever`, 4 in `test_missing_fields_become_none`, 5 in `test_neither_start_time_nor_cursor_raises` / `test_both_start_time_and_cursor_raises`.

**Open risk.** `_TIMESTAMP_FORMAT` (`%Y-%m-%dT%H:%M:%SZ`, no offset) is Zendesk's commonly documented shape but is unverified until Task 2's Q3 confirms it — Task 6 explicitly says to adjust if the spike shows otherwise. The rate-limit header format in `CLAUDE.md`/the design spec is speculative; Task 4 deliberately does *not* build a Zendesk-specific header parser up front (YAGNI — the shared engine's exponential-backoff fallback is safe even without it), so this is only revisited if Task 2's Q2 shows the plain fallback behaves badly in practice.
