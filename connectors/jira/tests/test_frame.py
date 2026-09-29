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
