# Jira Cloud Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the Jira Cloud connector: a unit-tested helper, skill, and example notebook that load Jira issues into a Spark DataFrame on AIDP, with a recorded live-test result.

**Architecture:** One helper module, `connectors/jira/jira.py`, using only `requests`. It searches `POST /rest/api/3/search/jql` with HTTP Basic auth (email + API token) and pages via the response's opaque `nextPageToken` — no offset, no client-built keyset (Jira manages the cursor server-side, unlike ServiceNow's `sys_id` scheme). A throwaway probe script answers the spike questions against a real site before the paging code is written; Task 2 is a hard gate, same discipline as the ServiceNow plan. Unit tests run offline against fake sessions.

**Tech Stack:** Python 3.11 (code stays compatible with 3.9+), `requests`, `pytest`, GitHub Actions, Claude Code plugin manifests, Jupyter notebook JSON.

**Spec:** `docs/specs/2026-09-28-oracle-aidp-connectors-design.md` (Jira Cloud connector section). Requirements: `connectors/jira/REQUIREMENTS.md`. Instructions: `CLAUDE.md`, `connectors/jira/CLAUDE.md`.

## Global Constraints

- Runtime contract: Spark 3.5, Python 3.11, Java 17.
- No dependency beyond `requests` on the cluster. Dev-only: `pytest`.
- Read-only. No writes to Jira, no webhooks, no OAuth 2.0 (3LO) in v1.
- Credentials come only from `JIRA_SITE`, `JIRA_EMAIL`, `JIRA_API_TOKEN`. Never printed, logged, or committed; use `<site>.atlassian.net` in examples.
- Unit tests are offline: no network, no Spark.
- Never call a real site from a unit test.
- Every request has a timeout. Retries on HTTP 429 are bounded and honour `Retry-After`.
- Never build or assume an offset; page with `nextPageToken` only.
- A caller-supplied JQL fragment containing `ORDER BY` is rejected before any request is sent — paging supplies its own ordering.
- One connector per pull request. Skills stay thin; logic lives in `connectors/jira/`.
- Never mark a live-test row PASS unless it actually ran. Otherwise NOT RUN with the reason.
- README carries verbatim: "Independent project by Arbisoft. Not affiliated with or endorsed by Oracle. Oracle and AI Data Platform are trademarks of Oracle Corporation."
- Label technical claims about Jira Cloud or AIDP as verified (observed in a live run, with date) or unverified.
- The old `GET/POST /rest/api/3/search` (offset pagination) is confirmed sunset (31 Oct 2025) and must not be used.

## Review Focus

The spec is silent on these inputs; each gets a test in the task shown.

1. **A page exactly fills `maxResults`, and the next page is empty** — must not loop or drop the boundary row; one extra request returning `isLast: true` is expected. (Task 5)
2. **The site returns HTML (redirected to a login page) with HTTP 200** — must raise a clear `JiraError`, not a raw `JSONDecodeError`. (Task 4)
3. **The server returns the same `nextPageToken` twice in a row** — must raise instead of looping forever. (Task 5)
4. **A custom field (`customfield_10019`) or an unset `assignee`/`reporter` (null person object)** — typed columns must become `None`, not crash on a missing key. (Task 6)
5. **A caller-supplied `jql` containing `order by` in any case, or one that already has an `AND`-joined clause** — must combine safely with the watermark clause without producing invalid JQL. (Task 3)

Also covered: `Retry-After` given as a plain integer vs. missing (Task 4), site given with a scheme (`https://`) or trailing slash (Task 1), timezone-aware datetimes (Task 3), an invalid-JQL error response shape (Task 4/6).

---

## File Structure

```
connectors/jira/
  jira.py                                   # the helper (single module)
  README.md
  spike/probe.py                            # THROWAWAY, labelled as such
  spike/_env.py                             # THROWAWAY .env loader (handles '=' and quotes)
  spike/RESULTS.md                          # verified facts from the probe
  tests/fakes.py                            # FakeResponse, FakeSession, FakeSearch
  tests/test_session.py
  tests/test_query.py
  tests/test_http.py
  tests/test_search.py
  tests/test_frame.py
  tests/test_notebook.py
  examples/jira_issue_load.ipynb
  live-results/RESULTS.md
  live-results/row1.json
skills/aidp-jira/SKILL.md
.claude-plugin/plugin.json                  # modify: add jira alongside servicenow entries if present
.claude-plugin/marketplace.json             # modify: add jira alongside servicenow entries if present
```

`jira.py` is one module, same reasoning as ServiceNow's: no intra-repo imports, so a user can upload the single file to an AIDP workspace and import it.

---

### Task 1: Scaffold and connection basics

**Files:**
- Create: `connectors/jira/jira.py`, `connectors/jira/tests/fakes.py`, `connectors/jira/tests/test_session.py`

**Interfaces:**
- Produces (in `jira.py`): `JiraError(Exception)`, `JiraAuthError(JiraError)`, `JiraRateLimitError(JiraError)`; `ENV_SITE`, `ENV_EMAIL`, `ENV_API_TOKEN` (str constants); `normalize_site(site: str) -> str`; `credentials_from_env() -> tuple[str, str, str]` returning `(site, email, api_token)`; `jira_session(email: str, api_token: str) -> requests.Session`; `redact(text: str, *secrets: str) -> str`.
- Produces (in `tests/fakes.py`): `FakeResponse(status=200, payload=None, headers=None)` with `.status_code`, `.headers`, `.json()` (raises `ValueError` when `payload is None`); `FakeSession(responses)` with `.post(url, json=None, timeout=None)` popping scripted responses in order and recording `.calls` as dicts `{"url","json","timeout"}`.

- [ ] **Step 1: Write the failing tests**

`connectors/jira/tests/fakes.py`:

```python
"""Test doubles for the Jira helper. No network, no Spark."""


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

    def post(self, url, json=None, timeout=None):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        return self._responses.pop(0)
```

`connectors/jira/tests/test_session.py`:

```python
import pytest

import jira as j


def test_normalize_site_strips_scheme_and_trailing_slash():
    assert j.normalize_site("https://awaisq.atlassian.net/") == "awaisq.atlassian.net"
    assert j.normalize_site("HTTP://awaisq.atlassian.net") == "awaisq.atlassian.net"
    assert j.normalize_site("  awaisq.atlassian.net  ") == "awaisq.atlassian.net"


@pytest.mark.parametrize("bad", ["", "   ", "https://", "awaisq.atlassian.net/jira", "a b"])
def test_normalize_site_rejects_bad_values(bad):
    with pytest.raises(ValueError):
        j.normalize_site(bad)


def test_credentials_from_env_returns_site_email_token(monkeypatch):
    monkeypatch.setenv("JIRA_SITE", "https://awaisq.atlassian.net/")
    monkeypatch.setenv("JIRA_EMAIL", "me@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok3n")
    assert j.credentials_from_env() == ("awaisq.atlassian.net", "me@example.com", "tok3n")


def test_credentials_from_env_names_the_missing_variable_only(monkeypatch):
    monkeypatch.setenv("JIRA_SITE", "awaisq.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "me@example.com")
    monkeypatch.delenv("JIRA_API_TOKEN", raising=False)
    with pytest.raises(j.JiraError) as exc:
        j.credentials_from_env()
    assert "JIRA_API_TOKEN" in str(exc.value)
    assert "me@example.com" not in str(exc.value)


def test_session_uses_basic_auth_and_json_accept_header():
    session = j.jira_session("me@example.com", "tok3n")
    assert session.auth == ("me@example.com", "tok3n")
    assert session.headers["Accept"] == "application/json"


def test_redact_replaces_every_secret_and_ignores_empty_ones():
    text = "failed for hunter2 and tok3n"
    assert j.redact(text, "hunter2", "tok3n", "") == "failed for *** and ***"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /Users/awais.qureshi/Documents/devstack/oracle-aidp-connectors && . .venv/bin/activate && PYTHONPATH=connectors/jira pytest connectors/jira/tests/test_session.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'jira'`.

- [ ] **Step 3: Write the minimal implementation**

`connectors/jira/jira.py`:

```python
"""Jira Cloud REST API v3 reader for AIDP notebooks.

Read-only. Uses only ``requests``. Credentials come from environment variables
and are never logged. See connectors/jira/REQUIREMENTS.md.
"""

from __future__ import annotations

import os
import re
from typing import Tuple

ENV_SITE = "JIRA_SITE"
ENV_EMAIL = "JIRA_EMAIL"
ENV_API_TOKEN = "JIRA_API_TOKEN"


class JiraError(Exception):
    """Any failure talking to Jira. Messages never contain credentials."""


class JiraAuthError(JiraError):
    """HTTP 401 or 403."""


class JiraRateLimitError(JiraError):
    """HTTP 429 persisted after the bounded number of retries."""


def normalize_site(site: str) -> str:
    """Return a bare host name from ``site`` or raise ValueError."""
    host = re.sub(r"^https?://", "", (site or "").strip(), flags=re.I).rstrip("/")
    if not host or "/" in host or " " in host:
        raise ValueError(
            "site must be a bare host name such as <site>.atlassian.net"
        )
    return host


def credentials_from_env() -> Tuple[str, str, str]:
    """Return ``(site, email, api_token)`` from the environment."""
    missing = [n for n in (ENV_SITE, ENV_EMAIL, ENV_API_TOKEN) if not os.environ.get(n)]
    if missing:
        raise JiraError("missing environment variable(s): " + ", ".join(missing))
    return (
        normalize_site(os.environ[ENV_SITE]),
        os.environ[ENV_EMAIL],
        os.environ[ENV_API_TOKEN],
    )


def jira_session(email: str, api_token: str):
    """A requests.Session with HTTP Basic auth (email:api_token) and JSON Accept."""
    import requests

    session = requests.Session()
    session.auth = (email, api_token)
    session.headers.update({"Accept": "application/json"})
    return session


def redact(text: str, *secrets: str) -> str:
    """Replace every non-empty secret in ``text`` with ``***``."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira/tests/test_session.py -q`
Expected: `10 passed`.

- [ ] **Step 5: Commit**

```bash
git add connectors/jira/jira.py connectors/jira/tests
git commit -m "feat(jira): scaffold credentials and session helpers"
```

---

### Task 2: Spike — answer the API questions against a real site (HARD GATE)

This task produces facts, not shipped code. `probe.py` is throwaway and labelled so. Do not start Task 3 until the decision in Step 5 is recorded.

**Files:**
- Create: `connectors/jira/spike/_env.py`, `connectors/jira/spike/probe.py`, `connectors/jira/spike/RESULTS.md`

**Interfaces:**
- Consumes: `jira.credentials_from_env`, `jira.jira_session` (Task 1).
- Produces: a written decision in `spike/RESULTS.md` that Tasks 3–6 rely on: (a) whether `search/jql` behaves as documented; (b) the real maximum `maxResults`; (c) how custom fields, person objects and timestamps appear; (d) the timezone of `updated`; (e) the shape of an invalid-JQL error.

- [ ] **Step 1: Get the gates (manual, not code)**

1. Confirm a Jira Cloud site exists (already have one: `awaisq.atlassian.net`, project `KAN`) and an API token from `id.atlassian.com/manage/api-tokens`.
2. Read Oracle's AIDP connector documentation and confirm there is no native `aidataplatform` type for Jira. If there is one, stop and revise the spec and requirements.
3. Confirm you can open a notebook on a running AIDP cluster.

Write `.env` at the repo root (already gitignored):

```
JIRA_SITE=awaisq.atlassian.net
JIRA_EMAIL=<your Atlassian account email>
JIRA_API_TOKEN=<the API token>
```

- [ ] **Step 2: Write the .env loader (handles `=` and quotes correctly — this exact class of bug cost significant time on the ServiceNow spike)**

`connectors/jira/spike/_env.py`:

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

`connectors/jira/spike/probe.py`:

```python
"""THROWAWAY spike probe. Not part of the shipped connector.

Answers the spike questions in connectors/jira/REQUIREMENTS.md against a real
site. Run from the repo root with .env set (see spike/_env.py):

    PYTHONPATH=connectors/jira:connectors/jira/spike python connectors/jira/spike/probe.py

Prints structure only. Never prints the site, email or token.
"""

import _env
_env.load(".env")

import jira as j

SITE, EMAIL, TOKEN = j.credentials_from_env()
SESSION = j.jira_session(EMAIL, TOKEN)
URL = "https://%s/rest/api/3/search/jql" % SITE


def call(**body):
    return SESSION.post(URL, json=body, timeout=60)


def q1_shape_and_paging(jql="order by updated asc", page=3, max_pages=200):
    seen, token, pages = [], None, 0
    prev_token = object()
    while pages < max_pages:
        body = {"jql": jql, "maxResults": page, "fields": ["key"]}
        if token:
            body["nextPageToken"] = token
        r = call(**body)
        if r.status_code != 200:
            print("Q1 FAIL: HTTP", r.status_code, r.text[:300])
            return
        payload = r.json()
        keys = [i["key"] for i in payload.get("issues", [])]
        seen += keys
        pages += 1
        is_last = payload.get("isLast")
        next_token = payload.get("nextPageToken")
        print("Q1 page=%d keys=%d isLast=%s has_token=%s" % (pages, len(keys), is_last, bool(next_token)))
        if is_last or not next_token:
            break
        if next_token == prev_token:
            print("Q1 FAIL: token did not advance")
            return
        prev_token = token
        token = next_token
    print("Q1 total=%d unique=%d" % (len(seen), len(set(seen))))


def q2_max_results():
    r = call(jql="order by updated asc", maxResults=10000, fields=["key"])
    body = r.json() if r.status_code == 200 else {}
    print("Q2 requested=10000 status=%d returned=%d" % (r.status_code, len(body.get("issues", []))))
    interesting = sorted(
        (k, v) for k, v in r.headers.items()
        if k.lower().startswith(("x-ratelimit", "retry-after"))
    )
    print("Q2 headers:", interesting)


def q3_field_shapes():
    r = call(jql="order by updated desc", maxResults=1,
             fields=["key", "assignee", "reporter", "updated", "created", "status", "priority"])
    if r.status_code != 200 or not r.json().get("issues"):
        print("Q3 FAIL or no issues:", r.status_code)
        return
    issue = r.json()["issues"][0]
    fields = issue.get("fields", {})
    shape = {k: (type(v).__name__, sorted(v) if isinstance(v, dict) else None) for k, v in fields.items()}
    print("Q3 field shapes:", shape)
    print("Q3 updated raw value:", fields.get("updated"))
    import datetime as dt
    print("Q3 utc_now:", dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))


def q4_invalid_jql():
    r = call(jql="this is not valid jql at all !!!", maxResults=1)
    print("Q4 invalid jql -> HTTP", r.status_code, r.text[:300])


def q5_custom_fields():
    r = call(jql="order by updated desc", maxResults=1, fields=["*all"])
    if r.status_code != 200 or not r.json().get("issues"):
        print("Q5 FAIL or no issues:", r.status_code)
        return
    fields = r.json()["issues"][0].get("fields", {})
    custom = sorted(k for k in fields if k.startswith("customfield_"))
    print("Q5 custom field keys present:", custom[:10], "total:", len(custom))


if __name__ == "__main__":
    q1_shape_and_paging()
    q2_max_results()
    q3_field_shapes()
    q4_invalid_jql()
    q5_custom_fields()
```

- [ ] **Step 4: Run the probe**

Run: `PYTHONPATH=connectors/jira:connectors/jira/spike python connectors/jira/spike/probe.py`
Expected: lines starting `Q1` (one or more page lines plus a total line), `Q2`, `Q3` (three lines), `Q4`, `Q5`. No site, email or token appears in the output.

- [ ] **Step 5: Record the findings and make the gate decision**

Create `connectors/jira/spike/RESULTS.md` by pasting the probe output under these headings, with today's date.

```markdown
# Jira Cloud API spike results

Date: <YYYY-MM-DD> · Site: awaisq.atlassian.net (host not otherwise recorded) · Run from: laptop

## Q1 search/jql shape and paging
Probe output: <paste Q1 lines>
Verified fact: <does isLast/nextPageToken behave as documented; total == unique?>

## Q2 maxResults limit and rate-limit headers
Probe output: <paste Q2 lines>
Verified fact: <the real maximum; which rate-limit headers exist, if any>

## Q3 field shapes and timestamp timezone
Probe output: <paste Q3 lines>
Verified facts:
- assignee/reporter shape: <dict keys, or null when unassigned>
- updated timestamp format and timezone: <e.g. ISO 8601 with offset>

## Q4 invalid JQL
HTTP status: <status> body shape: <paste, truncated>

## Q5 custom fields
Custom field key pattern: <e.g. customfield_NNNNN> sample present: <yes/no>

## Decision
<one of the two lines below>
```

If Q1 shows `total == unique`, `isLast` appears and behaves as expected, and no failure line was printed, write: `Proceed: search/jql pagination verified.` Continue to Task 3.

If Q1 printed `FAIL`, or totals disagree, write: `Stop: search/jql pagination not verified.` Do not continue. Revise the spec's paging section with the user, then rewrite Tasks 3–6 of this plan.

- [ ] **Step 6: Commit**

```bash
git add connectors/jira/spike
git commit -m "docs(jira): record API spike results (probe is throwaway)"
```

---

### Task 3: Query builder

**Files:**
- Modify: `connectors/jira/jira.py`
- Create: `connectors/jira/tests/test_query.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `build_jql(*, since=None, until=None, query=None) -> str`. `since` and `until` are `datetime` objects; naive values are treated as UTC, aware values are converted to UTC. The result is `(<query>) AND updated >= "<since>" AND updated <= "<until>" ORDER BY updated ASC, key ASC`, omitting either `updated` clause when its bound is `None`, and omitting the parenthesised query and its `AND` when `query` is `None`. Raises `ValueError` if `query` contains `order by` (case-insensitive, anywhere). Also `format_jql_timestamp(value: datetime) -> str` returning `YYYY-MM-DD HH:MM` in UTC (JQL's date-time literal format, minute precision — no seconds).

- [ ] **Step 1: Write the failing tests**

`connectors/jira/tests/test_query.py`:

```python
from datetime import datetime, timedelta, timezone

import pytest

import jira as j


def test_bare_query_only_orders_by_updated_then_key():
    assert j.build_jql() == "ORDER BY updated ASC, key ASC"


def test_all_conditions_combine_in_fixed_order():
    q = j.build_jql(
        query='project = KAN',
        since=datetime(2026, 9, 28, 10, 0, 0),
        until=datetime(2026, 9, 28, 12, 0, 0),
    )
    assert q == (
        '(project = KAN) AND updated >= "2026-09-28 10:00"'
        ' AND updated <= "2026-09-28 12:00" ORDER BY updated ASC, key ASC'
    )


def test_query_without_since_or_until():
    assert j.build_jql(query="project = KAN") == "(project = KAN) ORDER BY updated ASC, key ASC"


def test_aware_datetimes_are_converted_to_utc():
    tz = timezone(timedelta(hours=5))
    assert j.format_jql_timestamp(datetime(2026, 9, 28, 12, 0, 0, tzinfo=tz)) == "2026-09-28 07:00"


def test_naive_datetimes_are_treated_as_utc():
    assert j.format_jql_timestamp(datetime(2026, 9, 28, 7, 0, 0)) == "2026-09-28 07:00"


@pytest.mark.parametrize("bad", [
    "project = KAN order by created",
    "project = KAN ORDER BY created",
    "ORDER BY key",
])
def test_caller_query_with_order_by_is_rejected(bad):
    with pytest.raises(ValueError):
        j.build_jql(query=bad)
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira/tests/test_query.py -q`
Expected: FAIL with `AttributeError: module 'jira' has no attribute 'build_jql'`.

- [ ] **Step 3: Implement**

Add `from datetime import datetime, timezone` to the imports of `jira.py`, then append:

```python
_FORBIDDEN_IN_QUERY = re.compile(r"order\s+by", re.I)


def format_jql_timestamp(value: datetime) -> str:
    """UTC ``YYYY-MM-DD HH:MM`` (JQL date-time literal). Naive == UTC."""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%d %H:%M")


def build_jql(*, since=None, until=None, query=None) -> str:
    """JQL for one search. Caller query is ANDed with the watermark window."""
    if query and _FORBIDDEN_IN_QUERY.search(query):
        raise ValueError("query must not contain ORDER BY; paging adds its own ordering")
    parts = []
    if query:
        parts.append("(%s)" % query.strip())
    if since is not None:
        parts.append('updated >= "%s"' % format_jql_timestamp(since))
    if until is not None:
        parts.append('updated <= "%s"' % format_jql_timestamp(until))
    parts.append("ORDER BY updated ASC, key ASC")
    return " AND ".join(parts[:-1]) + (" " if parts[:-1] else "") + parts[-1]
```

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira -q`
Expected: all tests pass (10 from Task 1, 8 new = 18).

- [ ] **Step 5: Commit**

```bash
git add connectors/jira/jira.py connectors/jira/tests/test_query.py
git commit -m "feat(jira): JQL builder with rejected caller ORDER BY"
```

---

### Task 4: HTTP layer with bounded retry and clear errors

**Files:**
- Modify: `connectors/jira/jira.py`
- Create: `connectors/jira/tests/test_http.py`

**Interfaces:**
- Consumes: `JiraError`, `JiraAuthError`, `JiraRateLimitError` (Task 1); `FakeSession`, `FakeResponse` (Task 1).
- Produces: `MAX_BACKOFF_SECONDS = 60.0`; `post_json(session, url, body, *, timeout=60, max_retries=5, sleep=time.sleep) -> dict`. Behaviour: HTTP 429 sleeps `Retry-After` seconds (capped at `MAX_BACKOFF_SECONDS`), or `min(2**attempt, MAX_BACKOFF_SECONDS)` when the header is missing or not a number, retries up to `max_retries` times, then raises `JiraRateLimitError`; 401 or 403 raises `JiraAuthError`; any other status of 400 or above raises `JiraError` (its message includes the status and, for a 400, the response body text so an invalid-JQL error is visible to the caller); a body that is not JSON raises `JiraError` mentioning that the response was not JSON. Passes `timeout` to every call.

- [ ] **Step 1: Write the failing tests**

`connectors/jira/tests/test_http.py`:

```python
import pytest

import jira as j
from fakes import FakeResponse, FakeSession

URL = "https://awaisq.atlassian.net/rest/api/3/search/jql"


def test_returns_json_and_passes_timeout():
    session = FakeSession([FakeResponse(200, {"issues": []})])
    assert j.post_json(session, URL, {"jql": "x"}, timeout=7) == {"issues": []}
    assert session.calls[0]["timeout"] == 7
    assert session.calls[0]["json"] == {"jql": "x"}


def test_429_sleeps_retry_after_then_succeeds():
    sleeps = []
    session = FakeSession([
        FakeResponse(429, {}, {"Retry-After": "2"}),
        FakeResponse(200, {"issues": [1]}),
    ])
    assert j.post_json(session, URL, {}, sleep=sleeps.append) == {"issues": [1]}
    assert sleeps == [2.0]


def test_429_without_usable_retry_after_uses_capped_exponential_backoff():
    sleeps = []
    session = FakeSession([
        FakeResponse(429, {}, {"Retry-After": "not-a-number"}),
        FakeResponse(429, {}),
        FakeResponse(200, {"issues": []}),
    ])
    j.post_json(session, URL, {}, sleep=sleeps.append)
    assert sleeps == [1, 2]


def test_retry_after_is_capped():
    sleeps = []
    session = FakeSession([FakeResponse(429, {}, {"Retry-After": "99999"}), FakeResponse(200, {})])
    j.post_json(session, URL, {}, sleep=sleeps.append)
    assert sleeps == [j.MAX_BACKOFF_SECONDS]


def test_429_forever_raises_after_bounded_retries():
    session = FakeSession([FakeResponse(429, {}) for _ in range(4)])
    with pytest.raises(j.JiraRateLimitError):
        j.post_json(session, URL, {}, max_retries=3, sleep=lambda s: None)
    assert len(session.calls) == 4


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failures_raise_auth_error(status):
    with pytest.raises(j.JiraAuthError):
        j.post_json(FakeSession([FakeResponse(status, {})]), URL, {})


def test_invalid_jql_400_includes_body_text_in_error():
    resp = FakeResponse(400, {"errorMessages": ["Error in the JQL Query"]})
    with pytest.raises(j.JiraError) as exc:
        j.post_json(FakeSession([resp]), URL, {})
    assert "400" in str(exc.value)


def test_other_http_errors_raise_generic_error_with_status():
    with pytest.raises(j.JiraError) as exc:
        j.post_json(FakeSession([FakeResponse(500, {})]), URL, {})
    assert "500" in str(exc.value)


def test_non_json_200_raises_clear_error():
    with pytest.raises(j.JiraError) as exc:
        j.post_json(FakeSession([FakeResponse(200, None)]), URL, {})
    assert "not json" in str(exc.value).lower()
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira/tests/test_http.py -q`
Expected: FAIL with `AttributeError: module 'jira' has no attribute 'post_json'`.

- [ ] **Step 3: Implement**

Add `import time` to the imports of `jira.py`, then append:

```python
MAX_BACKOFF_SECONDS = 60.0


def _retry_after_seconds(response, fallback: float) -> float:
    value = response.headers.get("Retry-After")
    try:
        return min(max(float(value), 0.0), MAX_BACKOFF_SECONDS)
    except (TypeError, ValueError):
        return fallback


def post_json(session, url, body, *, timeout=60, max_retries=5, sleep=time.sleep) -> dict:
    """POST ``body`` to ``url`` and return the parsed JSON, with bounded 429 retries."""
    attempt = 0
    while True:
        response = session.post(url, json=body, timeout=timeout)
        status = response.status_code
        if status == 429:
            if attempt >= max_retries:
                raise JiraRateLimitError(
                    "rate limited (HTTP 429) after {} retries".format(max_retries)
                )
            sleep(_retry_after_seconds(response, min(2 ** attempt, MAX_BACKOFF_SECONDS)))
            attempt += 1
            continue
        if status in (401, 403):
            raise JiraAuthError(
                "HTTP {}: check the site, email and API token".format(status)
            )
        if status >= 400:
            raise JiraError(
                "HTTP {} from the Jira search API: {}".format(status, _safe_body(response))
            )
        try:
            return response.json()
        except ValueError:
            raise JiraError("response was not JSON; check the site name and URL") from None


def _safe_body(response) -> str:
    try:
        return str(response.json())[:300]
    except ValueError:
        return "<non-JSON body>"
```

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira -q`
Expected: all tests pass (18 + 10 = 28).

- [ ] **Step 5: Commit**

```bash
git add connectors/jira/jira.py connectors/jira/tests/test_http.py
git commit -m "feat(jira): HTTP layer with bounded 429 retry and clear errors"
```

---

### Task 5: `search_issues` cursor paging

Requires the Task 2 decision `Proceed`.

**Files:**
- Modify: `connectors/jira/jira.py`
- Modify: `connectors/jira/tests/fakes.py` (add `FakeSearch`)
- Create: `connectors/jira/tests/test_search.py`

**Interfaces:**
- Consumes: `build_jql`, `post_json`, `normalize_site`, `JiraError` (Tasks 1, 3, 4); `FakeSession`, `FakeResponse` (Task 1).
- Produces: `MAX_PAGE_SIZE = 5000`; `search_issues(session, site, *, query=None, fields=None, since=None, overlap_seconds=300, until=None, page_size=100, timeout=60, max_retries=5, sleep=time.sleep, now=None) -> Iterator[dict]`. `until` defaults to `now()` (or the current UTC time) evaluated once at the first request and reused for every page. The lower bound is `since - overlap_seconds` when `since` is given. Yields each issue dict from the `issues` array. The caller's next `since` is the `until` they passed in. Also `FakeSearch(pages, on_call=None)` in `tests/fakes.py`: a session double whose `.post()` ignores the request body's JQL/fields (the query builder is tested separately) and serves pre-built response pages by index, calling `on_call(call_number)` before serving each one — lets a test simulate a stuck cursor or exhaustion independent of real filtering.

- [ ] **Step 1: Add `FakeSearch` to the fakes**

Append to `connectors/jira/tests/fakes.py`:

```python
class FakeSearch:
    """Serves pre-built /search/jql response pages in order, by call count.

    Each page is a dict like {"issues": [...], "isLast": bool, "nextPageToken": str|None}.
    Ignores the request body — paging correctness is tested here, JQL construction
    is tested separately in test_query.py.
    """

    def __init__(self, pages, on_call=None):
        self.pages = list(pages)
        self.on_call = on_call
        self.calls = []

    def post(self, url, json=None, timeout=None):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        n = len(self.calls)
        if self.on_call:
            self.on_call(n)
        page = self.pages[n - 1] if n <= len(self.pages) else self.pages[-1]
        return FakeResponse(200, page)
```

- [ ] **Step 2: Write the failing tests**

`connectors/jira/tests/test_search.py`:

```python
from datetime import datetime

import pytest

import jira as j
from fakes import FakeResponse, FakeSession, FakeSearch

UNTIL = datetime(2026, 9, 28, 12, 0, 0)


def make_issue(n):
    return {"key": "KAN-%d" % n, "fields": {"updated": "2026-09-28T10:00:%02d.000+0000" % n}}


def keys(rows):
    return [r["key"] for r in rows]


def run(session, **kw):
    kw.setdefault("until", UNTIL)
    kw.setdefault("sleep", lambda s: None)
    return list(j.search_issues(session, "awaisq.atlassian.net", **kw))


def test_pages_through_every_issue_exactly_once():
    pages = [
        {"issues": [make_issue(1), make_issue(2)], "isLast": False, "nextPageToken": "t1"},
        {"issues": [make_issue(3), make_issue(4)], "isLast": False, "nextPageToken": "t2"},
        {"issues": [make_issue(5)], "isLast": True, "nextPageToken": None},
    ]
    session = FakeSearch(pages)
    assert keys(run(session, page_size=2)) == ["KAN-1", "KAN-2", "KAN-3", "KAN-4", "KAN-5"]
    assert len(session.calls) == 3


def test_is_last_true_stops_even_if_a_token_is_present():
    pages = [{"issues": [make_issue(1)], "isLast": True, "nextPageToken": "stale-token"}]
    session = FakeSearch(pages)
    assert len(run(session, page_size=10)) == 1
    assert len(session.calls) == 1


def test_empty_first_page_yields_nothing():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    assert run(session, page_size=10) == []
    assert len(session.calls) == 1


def test_missing_next_page_token_with_is_last_false_raises_instead_of_looping():
    session = FakeSearch([{"issues": [make_issue(1)], "isLast": False, "nextPageToken": None}])
    with pytest.raises(j.JiraError):
        run(session, page_size=10)


def test_repeated_token_raises_instead_of_looping_forever():
    pages = [
        {"issues": [make_issue(1)], "isLast": False, "nextPageToken": "same"},
        {"issues": [make_issue(2)], "isLast": False, "nextPageToken": "same"},
        {"issues": [make_issue(3)], "isLast": False, "nextPageToken": "same"},
    ]
    session = FakeSearch(pages)
    with pytest.raises(j.JiraError):
        run(session, page_size=10)


def test_upper_bound_is_fixed_for_every_page():
    pages = [
        {"issues": [make_issue(1)], "isLast": False, "nextPageToken": "t1"},
        {"issues": [make_issue(2)], "isLast": True, "nextPageToken": None},
    ]
    session = FakeSearch(pages)
    run(session, query="project = KAN", page_size=1)
    for call in session.calls:
        assert 'updated <= "2026-09-28 12:00"' in call["json"]["jql"]


def test_since_minus_overlap_is_the_lower_bound():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    run(session, since=datetime(2026, 9, 28, 10, 0, 5), overlap_seconds=2, page_size=10)
    assert 'updated >= "2026-09-28 10:00' in session.calls[0]["json"]["jql"]


def test_default_upper_bound_comes_from_injected_clock_once():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    list(j.search_issues(
        session, "awaisq.atlassian.net", page_size=10,
        now=lambda: datetime(2026, 9, 28, 12, 0, 0),
    ))
    assert 'updated <= "2026-09-28 12:00"' in session.calls[0]["json"]["jql"]


def test_request_body_shape_and_field_list_always_includes_key_and_updated():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    run(session, fields=["summary", "status"], page_size=25, timeout=9)
    call = session.calls[0]
    assert call["url"] == "https://awaisq.atlassian.net/rest/api/3/search/jql"
    assert call["json"]["maxResults"] == 25
    assert call["json"]["fields"] == ["key", "updated", "summary", "status"]
    assert "nextPageToken" not in call["json"]
    assert call["timeout"] == 9


def test_second_page_includes_next_page_token():
    pages = [
        {"issues": [make_issue(1)], "isLast": False, "nextPageToken": "abc"},
        {"issues": [], "isLast": True, "nextPageToken": None},
    ]
    session = FakeSearch(pages)
    run(session, page_size=1)
    assert session.calls[1]["json"]["nextPageToken"] == "abc"


def test_site_with_scheme_is_normalised():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    list(j.search_issues(session, "https://awaisq.atlassian.net/", until=UNTIL))
    assert session.calls[0]["url"].startswith("https://awaisq.atlassian.net/rest/api/3/")


@pytest.mark.parametrize("kwargs", [
    {"page_size": 0},
    {"page_size": 5001},
    {"query": "project = KAN order by created"},
])
def test_invalid_arguments_raise_value_error(kwargs):
    with pytest.raises(ValueError):
        run(FakeSession([]), **kwargs)
```

- [ ] **Step 3: Run to verify failure**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira/tests/test_search.py -q`
Expected: FAIL with `AttributeError: module 'jira' has no attribute 'search_issues'`.

- [ ] **Step 4: Implement**

Add `from datetime import datetime, timedelta, timezone` (extend the earlier datetime import) and `from typing import Iterator` to `jira.py`, then append:

```python
MAX_PAGE_SIZE = 5000


def search_issues(
    session,
    site,
    *,
    query=None,
    fields=None,
    since=None,
    overlap_seconds=300,
    until=None,
    page_size=100,
    timeout=60,
    max_retries=5,
    sleep=time.sleep,
    now=None,
) -> Iterator[dict]:
    """Yield every issue matching ``query`` and updated within a fixed window.

    Pages via the server's opaque ``nextPageToken``; never builds or assumes an
    offset. ``until`` is fixed at the first request, so issues updated during the
    read fall into the next run. Pass the same ``until`` as the next call's
    ``since``. De-duplicate on issue ``key``.
    """
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError("page_size must be between 1 and {}".format(MAX_PAGE_SIZE))
    build_jql(query=query)  # validates the caller's query before any request

    host = normalize_site(site)
    url = "https://{}/rest/api/3/search/jql".format(host)
    upper = until if until is not None else (now() if now else datetime.now(timezone.utc))
    lower = since - timedelta(seconds=overlap_seconds) if since is not None else None
    field_list = list(dict.fromkeys(["key", "updated", *(fields or [])]))
    jql = build_jql(since=lower, until=upper, query=query)

    token = None
    seen_tokens = set()
    while True:
        body = {"jql": jql, "fields": field_list, "maxResults": page_size}
        if token is not None:
            body["nextPageToken"] = token
        payload = post_json(
            session, url, body, timeout=timeout, max_retries=max_retries, sleep=sleep
        )
        yield from payload.get("issues", [])
        if payload.get("isLast"):
            return
        next_token = payload.get("nextPageToken")
        if not next_token:
            raise JiraError("server did not report isLast but returned no nextPageToken")
        if next_token in seen_tokens:
            raise JiraError("paging did not advance; the server repeated a nextPageToken")
        seen_tokens.add(next_token)
        token = next_token
```

- [ ] **Step 5: Run to verify pass**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira -q`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add connectors/jira/jira.py connectors/jira/tests
git commit -m "feat(jira): cursor-paged search_issues inside a fixed time window"
```

---

### Task 6: Typed DataFrames

Uses the verified field shapes and timezone from `spike/RESULTS.md`. If the spike shows `updated`'s format differs from `%Y-%m-%dT%H:%M:%S.%f%z`, adjust `_parse_timestamp`.

**Files:**
- Modify: `connectors/jira/jira.py`
- Create: `connectors/jira/tests/test_frame.py`

**Interfaces:**
- Consumes: `JiraError` (Task 1).
- Produces: `TYPED_FIELDS: list[tuple[str, str]]` — Spark DDL type names, covering `key STRING, summary STRING, status STRING, priority STRING, assignee STRING, reporter STRING, issuetype STRING, project STRING, created TIMESTAMP, updated TIMESTAMP`; `normalize_issue(issue: dict) -> tuple`; `to_dataframe(spark, issues)`. A person field (`assignee`/`reporter`) is a dict with `displayName`; `normalize_issue` takes `displayName`, or `None` when the field is `null` (unassigned). `status`/`priority`/`issuetype` are dicts with `name`; `project` is a dict with `key`. Missing or `None` fields become `None`. `TIMESTAMP` fields parse Jira's `%Y-%m-%dT%H:%M:%S.%f%z` format to aware datetimes and raise `JiraError` naming the field when unparseable. `to_dataframe` calls `spark.createDataFrame(data, schema=<DDL string>)`.

- [ ] **Step 1: Write the failing tests**

`connectors/jira/tests/test_frame.py`:

```python
from datetime import datetime, timezone

import pytest

import jira as j


class FakeSpark:
    def __init__(self):
        self.calls = []

    def createDataFrame(self, data, schema=None):
        self.calls.append((data, schema))
        return "df"


def as_dict(tup):
    return dict(zip([name for name, _ in j.TYPED_FIELDS], tup))


def test_normalize_issue_follows_field_order_and_unwraps_nested_objects():
    issue = {
        "key": "KAN-1",
        "fields": {
            "summary": "Fix the thing",
            "status": {"name": "In Progress"},
            "priority": {"name": "High"},
            "assignee": {"displayName": "Ann"},
            "reporter": None,
            "issuetype": {"name": "Bug"},
            "project": {"key": "KAN"},
            "created": "2026-09-01T08:00:00.000+0000",
            "updated": "2026-09-28T10:00:05.000+0000",
        },
    }
    out = as_dict(j.normalize_issue(issue))
    assert out["key"] == "KAN-1"
    assert out["status"] == "In Progress"
    assert out["assignee"] == "Ann"
    assert out["reporter"] is None
    assert out["project"] == "KAN"
    assert out["updated"] == datetime(2026, 9, 28, 10, 0, 5, tzinfo=timezone.utc)


def test_missing_fields_become_none():
    out = as_dict(j.normalize_issue({"key": "KAN-2", "fields": {}}))
    assert out["summary"] is None
    assert out["assignee"] is None
    assert out["created"] is None


def test_unparseable_timestamp_names_the_field():
    issue = {"key": "KAN-3", "fields": {"updated": "not-a-date"}}
    with pytest.raises(j.JiraError) as exc:
        j.normalize_issue(issue)
    assert "updated" in str(exc.value)


def test_to_dataframe_uses_ddl_from_typed_fields():
    spark = FakeSpark()
    issue = {"key": "KAN-1", "fields": {}}
    assert j.to_dataframe(spark, [issue]) == "df"
    data, ddl = spark.calls[0]
    assert len(data) == 1
    assert ddl.startswith("key STRING")
    assert "updated TIMESTAMP" in ddl


def test_to_dataframe_with_no_issues_still_passes_the_schema():
    spark = FakeSpark()
    j.to_dataframe(spark, [])
    data, ddl = spark.calls[0]
    assert data == []
    assert "summary STRING" in ddl
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira/tests/test_frame.py -q`
Expected: FAIL with `AttributeError: module 'jira' has no attribute 'TYPED_FIELDS'`.

- [ ] **Step 3: Implement**

Add `import json` is not needed here (no JSON-string fallback for Jira — every field is typed or dropped). Append to `jira.py`:

```python
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

TYPED_FIELDS = [
    ("key", "STRING"), ("summary", "STRING"), ("status", "STRING"),
    ("priority", "STRING"), ("assignee", "STRING"), ("reporter", "STRING"),
    ("issuetype", "STRING"), ("project", "STRING"),
    ("created", "TIMESTAMP"), ("updated", "TIMESTAMP"),
]

_NAME_FIELDS = {"status", "priority", "issuetype"}
_PERSON_FIELDS = {"assignee", "reporter"}


def _parse_jira_timestamp(name, text):
    try:
        return datetime.strptime(text, _TIMESTAMP_FORMAT)
    except (TypeError, ValueError):
        raise JiraError("field {} is not a Jira timestamp".format(name)) from None


def normalize_issue(issue: dict):
    """One issue as a tuple following TYPED_FIELDS order."""
    fields = issue.get("fields") or {}
    out = []
    for name, sql_type in TYPED_FIELDS:
        if name == "key":
            out.append(issue.get("key"))
            continue
        value = fields.get(name)
        if value is None:
            out.append(None)
        elif name in _NAME_FIELDS:
            out.append(value.get("name"))
        elif name in _PERSON_FIELDS:
            out.append(value.get("displayName"))
        elif name == "project":
            out.append(value.get("key"))
        elif sql_type == "TIMESTAMP":
            out.append(_parse_jira_timestamp(name, value))
        else:
            out.append(value)
    return tuple(out)


def to_dataframe(spark, issues):
    """A Spark DataFrame with one typed column per TYPED_FIELDS entry."""
    ddl = ", ".join("{} {}".format(n, t) for n, t in TYPED_FIELDS)
    data = [normalize_issue(i) for i in issues]
    return spark.createDataFrame(data, schema=ddl)
```

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira -q`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add connectors/jira/jira.py connectors/jira/tests/test_frame.py
git commit -m "feat(jira): typed issue DataFrame with unwrapped nested fields"
```

---

### Task 7: Skill, README, example notebook, plugin manifests

**Files:**
- Create: `skills/aidp-jira/SKILL.md`, `connectors/jira/README.md`, `connectors/jira/examples/jira_issue_load.ipynb`, `connectors/jira/tests/test_notebook.py`
- Create or modify: `.claude-plugin/plugin.json`, `.claude-plugin/marketplace.json` (create if this is the first connector to reach this task on `main`; check first — Task 2's spike step already confirmed no upstream plugin manifest work is pending)

**Interfaces:**
- Consumes: `search_issues`, `to_dataframe`, `credentials_from_env`, `jira_session` (Tasks 1, 5, 6).
- Produces: a notebook that runs top to bottom with only environment variables set, and a test that keeps committed notebooks free of outputs, real site names and inline tokens.

- [ ] **Step 1: Check whether the plugin manifests already exist**

Run: `test -f .claude-plugin/plugin.json && echo exists || echo missing`

If `missing`, create both files as shown in Step 5 below before continuing. If `exists`, read the file, and in Step 5 add a `jira` entry alongside whatever is already there instead of overwriting it.

- [ ] **Step 2: Write the failing notebook test**

`connectors/jira/tests/test_notebook.py`:

```python
import json
import re
from pathlib import Path

NOTEBOOKS = sorted(Path(__file__).resolve().parents[1].joinpath("examples").glob("*.ipynb"))


def test_at_least_one_example_notebook_exists():
    assert NOTEBOOKS


def test_notebooks_have_no_outputs_real_sites_or_inline_tokens():
    for path in NOTEBOOKS:
        nb = json.loads(path.read_text())
        assert nb["nbformat"] == 4
        for cell in nb["cells"]:
            if cell["cell_type"] == "code":
                assert cell["outputs"] == [], path.name
                assert cell["execution_count"] is None, path.name
            text = "".join(cell["source"])
            for site in re.findall(r"[\w-]+\.atlassian\.net", text):
                assert site == "<site>.atlassian.net", (path.name, site)
            assert not re.search(r"api_token\s*=\s*['\"][^'\"<]", text, re.I), path.name
```

- [ ] **Step 3: Run to verify failure**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira/tests/test_notebook.py -q`
Expected: FAIL on `test_at_least_one_example_notebook_exists` (no notebook yet).

- [ ] **Step 4: Write the notebook**

`connectors/jira/examples/jira_issue_load.ipynb`:

```json
{
 "nbformat": 4,
 "nbformat_minor": 5,
 "metadata": {"language_info": {"name": "python"}},
 "cells": [
  {"cell_type": "markdown", "metadata": {}, "id": "intro", "source": [
   "# Load Jira Cloud issues into a Delta table\n",
   "\n",
   "Full load on the first run, then incremental. Read-only against Jira Cloud.\n",
   "\n",
   "**Before you run**\n",
   "1. Upload `jira.py` to your workspace and set `HELPER_DIR` below to its folder.\n",
   "2. Provide `JIRA_SITE` (`<site>.atlassian.net`), `JIRA_EMAIL` and `JIRA_API_TOKEN`\n",
   "   (create one at https://id.atlassian.com/manage/api-tokens) as environment\n",
   "   variables for the cluster. Do not type the token into a cell.\n",
   "3. Set `TARGET` and `JQL` below."
  ]},
  {"cell_type": "code", "metadata": {}, "id": "config", "execution_count": null, "outputs": [], "source": [
   "import sys\n",
   "HELPER_DIR = \"<WORKSPACE_PATH_TO_HELPER_FOLDER>\"\n",
   "TARGET = \"<CATALOG>.<SCHEMA>.jira_issue\"\n",
   "JQL = \"project = KAN\"\n",
   "PAGE_SIZE = 100\n",
   "OVERLAP_SECONDS = 300\n",
   "sys.path.insert(0, HELPER_DIR)"
  ]},
  {"cell_type": "code", "metadata": {}, "id": "read", "execution_count": null, "outputs": [], "source": [
   "from datetime import datetime, timezone\n",
   "import jira as j\n",
   "\n",
   "site, email, token = j.credentials_from_env()\n",
   "session = j.jira_session(email, token)\n",
   "\n",
   "# Incremental: start from the newest row already loaded (None on the first run).\n",
   "since = None\n",
   "if spark.catalog.tableExists(TARGET):\n",
   "    since = spark.sql(f\"SELECT max(updated) AS m FROM {TARGET}\").first()[\"m\"]\n",
   "\n",
   "until = datetime.now(timezone.utc)  # fixed for this run\n",
   "issues = j.search_issues(\n",
   "    session, site, query=JQL,\n",
   "    since=since, until=until, overlap_seconds=OVERLAP_SECONDS, page_size=PAGE_SIZE,\n",
   ")\n",
   "df = j.to_dataframe(spark, list(issues))\n",
   "print(\"issues read:\", df.count())"
  ]},
  {"cell_type": "code", "metadata": {}, "id": "write", "execution_count": null, "outputs": [], "source": [
   "# The overlap window re-reads a few issues on purpose; MERGE on key removes duplicates.\n",
   "if not spark.catalog.tableExists(TARGET):\n",
   "    df.write.format(\"delta\").saveAsTable(TARGET)\n",
   "else:\n",
   "    df.createOrReplaceTempView(\"incoming_issue\")\n",
   "    spark.sql(\n",
   "        f\"MERGE INTO {TARGET} t USING incoming_issue s ON t.key = s.key \"\n",
   "        \"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *\"\n",
   "    )\n",
   "print(\"issues in target:\", spark.table(TARGET).count())"
  ]},
  {"cell_type": "markdown", "metadata": {}, "id": "limits", "source": [
   "## Limits\n",
   "- An `updated` watermark does not see Jira issue deletes.\n",
   "- An issue updated after `until` while the read is running is picked up by the\n",
   "  next run.\n",
   "- A personal/free Jira Cloud site is for learning and testing, not production\n",
   "  volume."
  ]}
 ]
}
```

- [ ] **Step 5: Run the notebook test, then write the skill, README, and manifests**

Run: `PYTHONPATH=connectors/jira pytest connectors/jira -q`
Expected: all tests pass, including the two new notebook tests.

`skills/aidp-jira/SKILL.md`:

```markdown
---
name: aidp-jira
description: Load Jira Cloud issue data into a Spark DataFrame from an AIDP notebook using the REST API v3 search/jql endpoint. Use when the user mentions Jira, Jira Cloud, issues, or JQL. Read-only; supports full and incremental loads.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-jira`: Jira Cloud issues to Spark

## When to use
- The user wants Jira Cloud issues in an AIDP notebook or Delta table.
- Not for writing to Jira, webhooks, or Jira Server/Data Center (on-prem).

## Prerequisites
1. Upload `connectors/jira/jira.py` to the workspace; add its folder to `sys.path`.
2. Environment variables `JIRA_SITE`, `JIRA_EMAIL`, `JIRA_API_TOKEN` set for the cluster (token from https://id.atlassian.com/manage/api-tokens). Never put the token in a cell.

## Use
Start from `connectors/jira/examples/jira_issue_load.ipynb`. The core calls:

    site, email, token = j.credentials_from_env()
    session = j.jira_session(email, token)
    issues = j.search_issues(session, site, query="project = KAN", since=since, until=until)
    df = j.to_dataframe(spark, list(issues))

## Gotchas
- Paging uses the server's opaque `nextPageToken` only. Never build or assume an offset — the old `/rest/api/3/search` endpoint (offset-based) was sunset by Atlassian on 31 October 2025 and no longer works.
- A caller `query` must not contain `ORDER BY`; paging supplies its own `ORDER BY updated ASC, key ASC`.
- A watermark does not detect Jira issue deletes, so deleted issues stay in the target.
- Issues updated after `until` while a read runs are picked up by the next run; the overlap window re-reads a few issues and MERGE on `key` de-duplicates them.
- Verified facts about maxResults, rate limits, field shapes and timezone: see `connectors/jira/spike/RESULTS.md` (each dated).
```

`connectors/jira/README.md`:

```markdown
# Jira Cloud connector

Read Jira Cloud issues into Spark on AIDP with `jira.py` (only `requests`).

Setup: see the skill `skills/aidp-jira/SKILL.md` and the example notebook in `examples/`.
Requirements and acceptance criteria: `REQUIREMENTS.md`. Verified API behaviour: `spike/RESULTS.md`.
Live-test status: `live-results/RESULTS.md`. Not supported until a PASS row exists there.

## Test

    pip install -r ../../requirements-dev.txt
    PYTHONPATH=. pytest -q
```

If `.claude-plugin/plugin.json` was `missing` in Step 1, create it and `marketplace.json` exactly as the ServiceNow plan's Task 7 shows (same content, this repo has one plugin manifest shared by every connector). If it already `exists` (e.g. from a resumed ServiceNow branch merged in the meantime), read it first and add a line to its description mentioning Jira rather than overwriting the file.

- [ ] **Step 6: Validate the plugin shape**

Run: `claude plugin validate .`
Expected: validation passes. If it reports a manifest-format error, fix the two JSON files to match the message and re-run; the manifest field names are unverified from memory.

- [ ] **Step 7: Commit**

```bash
git add skills connectors .claude-plugin
git commit -m "feat(jira): skill, README, example notebook, plugin manifests"
```

---

### Task 8: Live run on AIDP and recorded result

This task is manual. It produces the evidence the definition of done requires. If any step cannot be completed, record NOT RUN with the reason; do not record PASS.

**Files:**
- Create: `connectors/jira/live-results/RESULTS.md`, `connectors/jira/live-results/row1.json`
- Modify: `README.md`, `connectors/jira/README.md`, `skills/aidp-jira/SKILL.md` (gotchas learned)

**Interfaces:**
- Consumes: the notebook and helper (Tasks 5–7), a live Jira Cloud site and AIDP cluster (Task 2 gates).
- Produces: a dated PASS or NOT RUN row, and gotchas written into the skill and README.

- [ ] **Step 1: Work out how to supply the credentials on the cluster**

Find a way to give the notebook the three environment variables without typing the API token into a cell (cluster environment settings, or the AIDP credential store). Record the method that worked.

- [ ] **Step 2: Upload the helper and run the notebook**

Upload `connectors/jira/jira.py` to the workspace, set `HELPER_DIR`, `TARGET` and `JQL` in the notebook, attach it to a running cluster, and run all cells. Do not commit the executed notebook.

- [ ] **Step 3: Check the acceptance criteria**

- A1: `SELECT count(*) FROM <TARGET>` equals the issue count for the project shown in the Jira UI (or the total from Task 2's spike, adjusted for any issues added since).
- A2: rerun with `PAGE_SIZE = 3`; check `SELECT count(*), count(DISTINCT key) FROM <TARGET>` shows the two counts equal.
- A3: in the Jira UI, edit one issue, rerun the notebook, and confirm the read count is small (the changed issue plus overlap) and the target count is unchanged.
- A4 (forced 429) is proven offline by `test_429_forever_raises_after_bounded_retries`.
- A5 (caller `ORDER BY` rejected) is proven offline by `test_invalid_arguments_raise_value_error`.

- [ ] **Step 4: Record the result**

`connectors/jira/live-results/row1.json` (use placeholders, never the real site):

```json
{
  "connector": "jira",
  "status": "PASS",
  "date": "<YYYY-MM-DD>",
  "aidp_runtime": "<Spark/Python versions seen on the cluster>",
  "project": "KAN",
  "rows_loaded": 0,
  "credential_method": "<how the environment variables were supplied>",
  "notes": "<what happened, gotchas found>"
}
```

Replace every angle-bracket value and `0` with the real value. Use `"status": "NOT RUN"` and put the reason in `notes` if any acceptance check failed or could not run.

`connectors/jira/live-results/RESULTS.md`:

```markdown
# Live-test results

| # | Connector | Auth | Notebook | Status | Rows | Date |
|---|---|---|---|---|---|---|
| 1 | Jira Cloud | HTTP Basic (email + API token) | `../examples/jira_issue_load.ipynb` | <PASS or NOT RUN> | <n> | <YYYY-MM-DD> |

Notes: see `row1.json`. Site names, emails and tokens are never recorded here.
```

- [ ] **Step 5: Write the gotchas learned into the docs, update the root README, and commit**

Add each thing that surprised you on the live run (with today's date, labelled verified) to the Gotchas list in `skills/aidp-jira/SKILL.md`. If and only if the row is PASS, add a Jira Cloud line to the root `README.md`'s connector table: "Supported (live-tested <YYYY-MM-DD>)". Otherwise add it as "Experimental: NOT RUN" with the reason.

Run: `PYTHONPATH=connectors/jira pytest connectors/jira -q && claude plugin validate .`
Expected: all tests pass and the plugin validates.

```bash
git add README.md connectors skills
git commit -m "docs(jira): record live AIDP result and gotchas"
```

---

## Self-Review

**Spec coverage.** Purpose (Jira as active connector), definition of done, layout, session and credentials (Task 1), JQL builder with rejected caller `ORDER BY` (3), 429 retry, timeout, non-JSON/invalid-JQL errors (4), cursor paging and incremental window (5), typed DataFrame and nested-field unwrapping (6), skill, notebook and plugin manifests (7), spikes and live run and RESULTS row (2, 8). Requirements F1–F9 map to Tasks 1, 5, 5, 5, 4, 4, 6, 3, 4. Acceptance A1–A3 are live checks in Task 8; A4–A5 are offline tests; A6 is Task 8.

**Placeholder scan.** Angle-bracket values remain only where the value can exist only after a live run (spike output, result rows) or is a user-supplied path in the notebook config cell, matching the ServiceNow plan's convention. No step says "add error handling" or "similar to Task N" without the code.

**Type consistency.** `post_json(session, url, body, *, timeout, max_retries, sleep)` matches its use in `search_issues`. `search_issues(session, site, *, query, fields, since, overlap_seconds, until, page_size, ...)` matches its use in tests and the notebook. `TYPED_FIELDS`, `normalize_issue`, `to_dataframe(spark, issues)` match between Task 6 and the notebook. `jira_session(email, api_token)` matches `credentials_from_env`'s return order `(site, email, api_token)` everywhere it's unpacked.

**Review Focus.** All five items have a test: 1 in `test_pages_through_every_issue_exactly_once` / `test_is_last_true_stops_even_if_a_token_is_present`, 2 in `test_non_json_200_raises_clear_error`, 3 in `test_repeated_token_raises_instead_of_looping_forever`, 4 in `test_missing_fields_become_none` (null person/custom-field-shaped gaps), 5 in `test_caller_query_with_order_by_is_rejected` and `test_upper_bound_is_fixed_for_every_page` (AND-combination with a caller query).

**Lesson carried from the ServiceNow plan.** Task 2 includes a dedicated, tested `.env` loader (`spike/_env.py`) from the start, because the ServiceNow spike lost significant time to shell-based `.env` sourcing mishandling `=` and `$` inside values. The probe imports it directly; no shell `source`/`export` step appears anywhere in this plan.

**Open risk.** The plugin manifest field names in Task 7 are from memory, same caveat as the ServiceNow plan; Step 6 validates them. The exact `updated`/`created` timestamp format (`_TIMESTAMP_FORMAT`) is Jira's commonly documented shape but is unverified until Task 2's Q3 confirms it — Task 6 explicitly says to adjust it if the spike shows otherwise.
