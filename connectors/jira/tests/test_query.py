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
