-- name: edge-insert-view
-- Records a campaign view received from the tracking edge. Views for unknown
-- campaigns insert nothing.
INSERT INTO campaign_views (campaign_id, subscriber_id, created_at)
SELECT c.id, s.id, $3::TIMESTAMPTZ FROM campaigns c
LEFT JOIN subscribers s ON s.uuid = NULLIF($2, '')::UUID
WHERE c.uuid = $1::UUID;

-- name: edge-insert-click
-- Records a link click received from the tracking edge. Mirrors register-link-click
-- but keeps the edge-side timestamp.
INSERT INTO link_clicks (campaign_id, subscriber_id, link_id, created_at)
SELECT c.id, s.id, l.id, $4::TIMESTAMPTZ FROM links l
LEFT JOIN campaigns c ON c.uuid = NULLIF($2, '')::UUID
LEFT JOIN subscribers s ON s.uuid = NULLIF($3, '')::UUID
WHERE l.uuid = $1::UUID;

-- name: edge-get-sync-state
SELECT last_seq FROM edge_sync_state WHERE edge_id = $1;

-- name: edge-set-sync-state
INSERT INTO edge_sync_state (edge_id, last_seq, updated_at) VALUES ($1, $2, NOW())
ON CONFLICT (edge_id) DO UPDATE SET last_seq = EXCLUDED.last_seq, updated_at = NOW();
