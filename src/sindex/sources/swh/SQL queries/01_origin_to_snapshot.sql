-- Step 1: Origin → Visit → Snapshot
-- Step 0: Register Software Heritage 2025 dataset (if not already present)

CREATE EXTERNAL TABLE IF NOT EXISTS swh_graph_2025_10_08.origin (
    id STRING,
    url STRING
)
STORED AS PARQUET
LOCATION 's3://softwareheritage/graph/2025-10-08/origin/';

CREATE TABLE default.url_and_date AS
SELECT
    o.url,
    ovs.date as visit_date
FROM swh_graph_2025_10_08.origin o
JOIN swh_graph_2025_10_08.origin_visit_status ovs
ON o.url = ovs.origin;


CREATE TABLE default.url_date_snapshot_2a AS
SELECT
    u.url,
    u.visit_date,
    ovs.snapshot as snapshot_id
FROM default.url_and_date u
JOIN swh_graph_2025_10_08.origin_visit_status ovs
    ON u.url = ovs.origin
    AND u.visit_date = ovs.date
WHERE ovs.snapshot IS NOT NULL;