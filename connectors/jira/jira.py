"""Jira Cloud REST API v3 reader for AIDP notebooks.

Read-only. Uses only ``requests``. Credentials come from environment variables
and are never logged. See connectors/jira/REQUIREMENTS.md.
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
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
