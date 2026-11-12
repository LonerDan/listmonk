# Tracking Edge API

This document describes the API a tracking edge service must implement so that
a private listmonk instance can use it for link click and campaign open tracking.

## Overview

listmonk runs in a private network and opens no inbound ports. The edge runs on
a machine with a public address and does two things:

1. **Public side** (internet-facing): serves the tracking URLs embedded in
   campaign emails: link redirects and open-tracking pixels. It records each hit.
2. **Internal side** (VPN only): listmonk calls this API over the VPN to register
   links, push tracking config, and pull recorded events.

Connectivity is one-way. listmonk dials out to the edge's internal listener.
The edge never connects to listmonk.

```
 recipient ──https──▶ edge public port  ◀── records events
                         │
 listmonk ──VPN──▶ edge internal port (PUT links/config, GET events, POST ack)
```

### Requirements

- **Storage: SQLite.** The edge stores its data in a SQLite database. SQLite is
  the reference store because the edge is a single self-contained process: the
  database survives restarts without losing events or links, and needs no
  separate infrastructure. Recommended pragmas: `journal_mode=WAL`,
  `synchronous=NORMAL`, `busy_timeout`. Suggested schema:

  ```sql
  CREATE TABLE IF NOT EXISTS links (
      uuid TEXT PRIMARY KEY,
      url  TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS events (
      seq       INTEGER PRIMARY KEY, -- monotonic counter, persisted across restarts
      type      TEXT NOT NULL,       -- 'view' or 'click'
      at        TEXT NOT NULL,       -- RFC 3339
      camp_uuid TEXT NOT NULL,
      sub_uuid  TEXT NOT NULL DEFAULT '',
      link_uuid TEXT NOT NULL DEFAULT ''
  );

  CREATE TABLE IF NOT EXISTS meta (
      key   TEXT PRIMARY KEY, -- 'edge_id', 'links_version', ...
      value TEXT NOT NULL
  );
  ```

- Two listeners: a public one (any address) and an internal one that is bound
  only to the VPN interface or a VPN-only address. The internal listener must
  not be reachable from the internet.
- The public listener serves only the two public routes below.
- The internal listener serves only the `/internal/v1/*` routes below.
- Persistent storage (SQLite, see above) for the link map, the event queue, the
  edge ID, and the `links_version` fingerprint. The edge must survive restarts
  without losing events or links.

## Public routes

These are unauthenticated and are called by recipients' mail clients and browsers.

### `GET /link/{link_uuid}/{campaign_uuid}/{subscriber_uuid}`

Click tracking.

- Look up `link_uuid` in the link map.
- If found:
  - Record a `click` event (see [Event format](#event-format)) with `link_uuid`,
    `camp_uuid`, `sub_uuid`, and `at` set to the hit time.
  - Respond `307 Temporary Redirect` with `Location` set to the stored URL.
- If not found: respond `404 Not Found`. Do not record an event.

The campaign and subscriber UUIDs are recorded as received. listmonk validates
them when it pulls the event, so the edge does not need to check them against
anything.

### `GET /campaign/{campaign_uuid}/{subscriber_uuid}/px.png`

Open tracking.

- Record a `view` event with `camp_uuid`, `sub_uuid`, and `at` set to the hit time.
- Always respond `200 OK` with `Content-Type: image/png`. The body is a 1x1
  transparent PNG. Respond with the pixel even when recording fails or the
  request is malformed, so the response never breaks the email display.

### Recording and tracking disabled

If the edge is told by `PUT /internal/v1/config` that tracking is disabled
(`disable_tracking: true`):

- `/link/...` still redirects to the stored URL if the link is known, but does not record.
- `/campaign/.../px.png` still returns the pixel, but does not record.

## Internal routes

All `/internal/v1/*` routes require:

```
Authorization: Bearer <token>
```

The token is a shared secret configured on both sides (`tracking_edge.token` in
listmonk). Respond `401 Unauthorized` if it is missing or wrong.

Request and response bodies are JSON (`Content-Type: application/json`).
Error responses should be a non-2xx status code with a JSON body such as
`{"error": "message"}`. listmonk treats any non-2xx response as a failure and
logs it.

### `GET /internal/v1/health`

Returns the edge's identity and status. listmonk calls this at the start of
every sync cycle and uses `edge_id` to key its sync cursor.

Response `200 OK`:

```json
{
  "edge_id": "edge-eu-1",
  "links": 1234,
  "queued_events": 56,
  "last_seq": 98765,
  "links_version": "9f2c1a…"
}
```

- `edge_id` (string, required): a stable, unique ID for this edge. It must be
  persisted and must not change across restarts. If it changes, listmonk stops
  syncing with an error. Changing it causes events to be re-read from seq 0,
  so it must not be changed casually.
- `links`, `queued_events`, `last_seq` (integers, optional): informational.
  listmonk does not depend on them.
- `links_version` (string, required): an opaque, ETag-style fingerprint of the
  link map (see [`PUT /internal/v1/links`](#put-internalv1links)). listmonk
  compares it to the version it last synced and re-pushes all links when they
  differ. It must be persisted in the SQLite `meta` table and recomputed on
  every change to the link map. A stable recipe: SHA-256 hex digest of the
  sorted `uuid:url` lines of the link map (the empty map has a well-defined,
  non-empty digest).

### `PUT /internal/v1/config`

Pushes tracking behaviour from listmonk. listmonk sends this at the start of
every sync cycle (about every `poll_interval`), so the edge always has the latest value.

Request:

```json
{
  "disable_tracking": false,
  "individual_tracking": true
}
```

- `disable_tracking` (bool): when true, stop recording events (see
  [Recording and tracking disabled](#recording-and-tracking-disabled)).
- `individual_tracking` (bool): listmonk applies this when it pulls events.
  The edge records the subscriber UUID as received and does not need to act on it.

Response: `204 No Content`.

### `PUT /internal/v1/links`

Registers links the edge should redirect. listmonk calls this when it creates a
tracked link during campaign rendering, before the link is put into an email.
A link is therefore always registered before any recipient can click it.

Request:

```json
{
  "links": [
    { "uuid": "5b1c2d3e-0000-4000-8000-000000000001", "url": "https://example.com/offer?x=1" }
  ]
}
```

- `links` (array, required, may contain one or more items).
- `uuid` (string): the link UUID. Treat it as the primary key.
- `url` (string): the destination. Store it exactly as received.

Behaviour:

- Upsert by `uuid`. Re-sending a known link with the same URL is harmless.
  Re-sending with a different URL replaces the stored URL.
- Persist before responding, so a restart does not lose links.
- Recompute and persist `links_version` after applying the batch.
- Response: `200 OK` with the fingerprint of the link map after the update:

  ```json
  { "links_version": "9f2c1a…" }
  ```

  listmonk stores this value and compares it against `links_version` from
  `GET /health` on every sync cycle. If they differ — for example, because the
  edge's SQLite database was wiped or restored from an old backup — listmonk
  re-pushes every link it knows. This is what makes edge data loss self-healing
  instead of requiring a listmonk restart.

### `GET /internal/v1/events?after={seq}&limit={n}`

Returns recorded events with `seq > after`, in ascending `seq` order, up to `limit`.

Query parameters:

- `after` (integer, optional, default `0`): return events with `seq` strictly greater than this value.
- `limit` (integer, optional, default `100`, max `1000`). listmonk requests
  `limit=500`.

Response `200 OK`:

```json
{
  "edge_id": "edge-eu-1",
  "last_seq": 98765,
  "events": [
    {
      "seq": 98001,
      "type": "click",
      "at": "2026-11-12T12:00:03Z",
      "camp_uuid": "11111111-1111-1111-1111-111111111111",
      "sub_uuid": "22222222-2222-2222-2222-222222222222",
      "link_uuid": "33333333-3333-3333-3333-333333333333"
    },
    {
      "seq": 98002,
      "type": "view",
      "at": "2026-11-12T12:00:05Z",
      "camp_uuid": "11111111-1111-1111-1111-111111111111",
      "sub_uuid": "22222222-2222-2222-2222-222222222222"
    }
  ]
}
```

- `edge_id` (string, required): must match `GET /internal/v1/health`.
- `last_seq` (integer, required): the highest `seq` held by the edge, or the
  current cursor. Informational.
- `events` (array): may be empty. An empty array means there is nothing new.
- listmonk stops paging when it gets an empty page or fewer than 500 events.
  Return up to `limit` events whenever they are available, so a full page is
  never mistaken for the end of the queue.

Behaviour:

- Events are not removed by this call. They stay until acknowledged (see below).
  The same event may be returned more than once, so listmonk must be able to
  handle duplicate delivery.

### `POST /internal/v1/events/ack`

Tells the edge that every event with `seq <= up_to` has been durably stored by listmonk.

Request:

```json
{ "up_to": 98765 }
```

Behaviour:

- Delete every event with `seq <= up_to` from the queue.
- Acking is idempotent. Acking a value lower than a previous ack is a no-op.
- Response: `204 No Content`.

## Event format

Each event is one recorded hit.

| Field        | Type   | Required | Notes                                                    |
|--------------|--------|----------|----------------------------------------------------------|
| `seq`        | int64  | yes      | Monotonic, unique, never reused, starts at 1. Persist the counter across restarts. |
| `type`       | string | yes      | `"view"` (pixel) or `"click"` (link redirect).           |
| `at`         | string | yes      | RFC 3339 time of the hit, UTC recommended.               |
| `camp_uuid`  | string | yes      | Campaign UUID from the URL, as received.                 |
| `sub_uuid`   | string | yes      | Subscriber UUID from the URL, as received.               |
| `link_uuid`  | string | click only | Link UUID from the URL. Omit it for `view` events.     |

listmonk skips events it cannot use, such as non-UUID values, and logs them. A
skipped event still advances the cursor, so a bad event does not block the queue.
The edge does not need to validate the UUIDs.

## Delivery semantics

- **At-least-once.** listmonk pulls events, stores them in one DB transaction
  together with its cursor, then acks. If listmonk crashes after the commit but
  before the ack, the events are returned again. listmonk dedupes using the
  cursor, so duplicates are not double-counted.
- **Ordering.** Events are returned in ascending `seq`. The edge must assign
  `seq` in the order it records events.
- **Cursor.** listmonk keeps its own cursor per `edge_id`. The edge does not need
  to track listmonk's position beyond the acked high-water mark.
- **Retention.** Unacked events must be kept. The edge may need a cap or alert
  if listmonk is offline for long periods. It should not drop unacked events
  silently.

## Expected sequence per sync cycle

1. `PUT /internal/v1/config`
2. `GET /internal/v1/health` (read `edge_id` and `links_version`)
3. If `links_version` differs from the last synced version, re-push all known
   links with `PUT /internal/v1/links` (batched) and store the returned version.
4. Loop:
   - `GET /internal/v1/events?after=<cursor>&limit=500`
   - If empty, stop.
   - Store the events and advance the cursor, all in one transaction.
   - `POST /internal/v1/events/ack` with `up_to` set to the new cursor.
   - If fewer than 500 events were returned, stop.

New links are additionally pushed individually, when listmonk first renders a
tracked link: `PUT /internal/v1/links`. This happens at render time, not on
the cycle above. (listmonk does not store the version from these individual
pushes; the next health check confirms convergence.)

## Failure behaviour on the listmonk side

- If the edge is unreachable when listmonk creates a link, it pushes nothing
  and falls back to the raw URL for 30 seconds before retrying. The recipient
  then gets an untracked link for that send.
- If a sync cycle fails, listmonk logs the error and retries on the next cycle.
  Events are not lost because they stay on the edge until acked.

## Link map recovery

If the edge loses its link map (its SQLite database is wiped or restored from
a stale backup), the mismatch between the edge's `links_version` and the
version listmonk last synced is detected on the next sync cycle, and listmonk
re-pushes every link it knows. Tracking resumes without operator intervention
and without a listmonk restart. Clicks that arrived while the map was empty
still return 404 and are lost — this window is bounded by `poll_interval`.

## Configuration reference (listmonk side)

```toml
[tracking_edge]
enabled = true
public_url = "https://t.example.com"     # Used in emails for /link and /campaign URLs
internal_url = "http://10.8.0.2:8081"    # Edge internal listener, over the VPN
token = "change-me"                      # Bearer token for /internal/v1/*
poll_interval = "30s"
```

When `enabled` is true, listmonk builds tracking URLs from `public_url`. Message
and archive URLs still use `app.root_url`.

## Quick check with curl

```sh
TOKEN=change-me
EDGE=http://10.8.0.2:8081

curl -sS -H "Authorization: Bearer $TOKEN" $EDGE/internal/v1/health
curl -sS -X PUT -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"links":[{"uuid":"5b1c2d3e-0000-4000-8000-000000000001","url":"https://example.com"}]}' \
  $EDGE/internal/v1/links
curl -sS -H "Authorization: Bearer $TOKEN" "$EDGE/internal/v1/events?after=0&limit=10"
curl -sS -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"up_to":0}' $EDGE/internal/v1/events/ack
```
