# Jira Cloud API spike results

Date: 2026-09-29 · Site: awaisq.atlassian.net (host not otherwise recorded) · Run from: laptop

## Q1 search/jql shape and paging

Probe output (page size 3, 13 total issues, bounded query
`updated >= "2000-01-01 00:00" ORDER BY updated ASC`):

```
Q1 page=1 keys=3 isLast=False has_token=True
Q1 page=2 keys=3 isLast=False has_token=True
Q1 page=3 keys=3 isLast=False has_token=True
Q1 page=4 keys=3 isLast=False has_token=True
Q1 page=5 keys=1 isLast=True has_token=False
Q1 total=13 unique=13
```

**Verified fact:** `POST /rest/api/3/search/jql` behaves exactly as documented.
`nextPageToken` advances correctly, `isLast` flips to `true` on the final page
and `nextPageToken` is absent there, and paging is lossless (13 total, 13
unique, in the order returned).

**New finding, not in the original design:** a JQL query with no restricting
clause is rejected outright: `HTTP 400 {"errorMessages":["Unbounded JQL
queries are not allowed here. Please add a search restriction to your
query."]}`. This does not affect the shipped connector, because
`search_issues` always includes an `updated <= "<until>"` bound (`until`
defaults to the current time, never omitted from the JQL) — but the fact is
recorded so nobody later builds a bare `search_issues()` call assuming an
unrestricted default query works.

## Q2 maxResults limit and rate-limit headers

```
Q2 requested=5000 status=200 body={"issues":[...
Q2 requested=5001 status=400 body={"errorMessages":["The max results parameter has to be between 1 and 5,000."],"errors":{}}
Q2 headers: [('X-Ratelimit-Limit', '200'), ('X-Ratelimit-Remaining', '197')]
```

**Verified fact:** the maximum `maxResults` is exactly **5000**. A value above
it is rejected with a precise, named error — not silently capped. This
matches the `MAX_PAGE_SIZE = 5000` already used in the plan; no code change
needed. Rate-limit headers `X-Ratelimit-Limit` / `X-Ratelimit-Remaining` are
present and decrement per request (observed 200 → 197 across a handful of
calls). No `Retry-After` header was observed on a 200 response, as expected;
this site was never actually rate-limited during testing, so the 429 path
itself (`post_json`'s retry loop) remains verified only by the offline unit
tests, not by a live 429.

## Q3 field shapes and timestamp timezone

```
Q3 field shapes: {'reporter': ('dict', ['accountId', 'accountType', 'active', 'avatarUrls', 'displayName', 'emailAddress', 'self', 'timeZone']), 'assignee': ('NoneType', None), 'priority': ('NoneType', None), 'updated': ('str', None), 'created': ('str', None), 'status': ('dict', ['description', 'iconUrl', 'id', 'name', 'self', 'statusCategory'])}
Q3 updated raw value: 2026-09-28T19:32:13.094+0500
Q3 utc_now: 2026-09-29T04:46:16
```

**Verified facts:**
- `reporter` is a dict with `displayName` (and other keys we don't use).
  `assignee` and `priority` were `null` on this issue (unassigned/unset) —
  our null-check-first normalization handles this correctly regardless of
  field type.
- `status` is a dict with `name` (matches the design's assumption).
- **Timestamps are NOT UTC.** `updated` came back as
  `2026-09-28T19:32:13.094+0500` — a +05:00 offset, not `+0000`. The format
  `%Y-%m-%dT%H:%M:%S.%f%z` parses this correctly regardless of which offset is
  present (Python's `%z` accepts any `+HHMM`/`-HHMM` offset, and aware-datetime
  comparisons are offset-independent), so **no code change is required** in
  `_parse_jira_timestamp` — but the design's original assumption of a UTC
  offset was wrong and is corrected here for anyone reading the skill later.

## Q4 invalid JQL

HTTP status: 400. Body:
```
{"errorMessages":["Error in the JQL Query: Expecting either 'OR' or 'AND' but got 'jql'. (line 1, character 19)"],"errors":{}}
```

**Verified fact:** an invalid JQL string returns HTTP 400 with a JSON body
containing `errorMessages` (an array of human-readable strings). This matches
`post_json`'s existing error handling, which includes the response body text
in the raised `JiraError` for any 4xx/5xx status — no code change needed.

## Q5 custom fields

Custom field key pattern confirmed: `customfield_NNNNN` (5-digit numeric ID).
Sample from this site: `customfield_10001`, `customfield_10015`,
`customfield_10017`, `customfield_10019`, `customfield_10021`,
`customfield_10026` (6 total). The shipped connector does not surface custom
fields by default (`TYPED_FIELDS` covers only common fields); a caller who
needs one passes its `customfield_NNNNN` key via the `fields` argument to
`search_issues`, and it will appear in the raw `issue["fields"]` dict — but
`to_dataframe`/`normalize_issue` in this v1 do not have a typed column for it
and will silently drop it, since `normalize_issue` only reads names in
`TYPED_FIELDS`. **This is a real v1 limitation to document in the skill's
gotchas**, not something this spike needs to fix.

## Decision

**Proceed: search/jql pagination verified.** Continuing to Tasks 5 and 6 with
one addition to the skill's documented gotchas (unbounded-query rejection,
non-UTC timestamps, custom fields silently dropped from typed output) and no
changes to the paging or timestamp-parsing code already written in Tasks 3–4.
