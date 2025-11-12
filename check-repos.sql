-- Quick overview: All repos with post counts and backfill status
SELECT 
    rp.repo_did,
    rp.backfill_complete,
    rp.last_seq,
    rp.updated_at,
    COALESCE(post_counts.post_count, 0) as post_count,
    rp.last_snapshot_row_count,
    rp.last_snapshot_parquet_count
FROM repo_progress rp
LEFT JOIN (
    SELECT repo_did, COUNT(*) as post_count
    FROM emoji_post
    GROUP BY repo_did
) post_counts ON rp.repo_did = post_counts.repo_did
ORDER BY rp.updated_at DESC;

-- Just the repo DIDs that have posts
SELECT DISTINCT repo_did 
FROM emoji_post 
ORDER BY repo_did;

-- Repos with post counts (only repos that have posts)
SELECT 
    repo_did,
    COUNT(*) as post_count,
    MIN(created_at) as first_post,
    MAX(created_at) as last_post
FROM emoji_post
GROUP BY repo_did
ORDER BY post_count DESC;

-- Check if a specific repo exists
-- SELECT * FROM repo_progress WHERE repo_did = 'did:plc:...';

-- Repos that are marked as complete but might have validation issues
SELECT 
    rp.repo_did,
    rp.backfill_complete,
    rp.last_snapshot_row_count,
    rp.last_snapshot_parquet_count,
    COUNT(ep.*) as actual_post_count
FROM repo_progress rp
LEFT JOIN emoji_post ep ON rp.repo_did = ep.repo_did
WHERE rp.backfill_complete = true
GROUP BY rp.repo_did, rp.backfill_complete, rp.last_snapshot_row_count, rp.last_snapshot_parquet_count
ORDER BY rp.repo_did;

