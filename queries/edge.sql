-- name: edge-insert-view
-- Records a campaign view received from the tracking edge. Views for unknown
-- campaigns insert nothing. NULLIF + regex guard makes the ::UUID casts safe
-- for arbitrary input (though the caller validates UUIDs as well).
INSERT INTO campaign_views (campaign_id, subscriber_id, created_at)
SELECT c.id, s.id, $3::TIMESTAMPTZ FROM campaigns c
LEFT JOIN subscribers s ON s.uuid = CASE WHEN NULLIF($2, '') ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN NULLIF($2, '')::UUID END
WHERE c.uuid = CASE WHEN $1 ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN $1::UUID END;

-- name: edge-insert-click
-- Records a link click received from the tracking edge. Mirrors register-link-click
-- but keeps the edge-side timestamp. NULLIF + regex guard makes the ::UUID casts
-- safe for arbitrary input (though the caller validates UUIDs as well).
INSERT INTO link_clicks (campaign_id, subscriber_id, link_id, created_at)
SELECT c.id, s.id, l.id, $4::TIMESTAMPTZ FROM links l
LEFT JOIN campaigns c ON c.uuid = CASE WHEN NULLIF($2, '') ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN NULLIF($2, '')::UUID END
LEFT JOIN subscribers s ON s.uuid = CASE WHEN NULLIF($3, '') ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN NULLIF($3, '')::UUID END
WHERE l.uuid = CASE WHEN $1 ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN $1::UUID END;

-- name: edge-get-sync-state
SELECT last_seq, links_version FROM edge_sync_state WHERE edge_id = $1;

-- name: edge-set-sync-state
INSERT INTO edge_sync_state (edge_id, last_seq, links_version, updated_at) VALUES ($1, $2, '', NOW())
ON CONFLICT (edge_id) DO UPDATE SET last_seq = EXCLUDED.last_seq, updated_at = NOW();

-- name: edge-set-links-version
INSERT INTO edge_sync_state (edge_id, last_seq, links_version, updated_at) VALUES ($1, 0, $2, NOW())
ON CONFLICT (edge_id) DO UPDATE SET links_version = EXCLUDED.links_version, updated_at = NOW();

-- name: edge-get-links
SELECT uuid, url FROM links;
