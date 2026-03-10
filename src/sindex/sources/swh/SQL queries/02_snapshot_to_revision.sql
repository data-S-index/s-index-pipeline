-- Step 2: Snapshot → Main/Master → Revision

CREATE TABLE default.snapshot_branch_filtered AS
SELECT
    snapshot_id,
    target AS revision_id
FROM swh_graph_2025_10_08.snapshot_branch
WHERE target_type = 'revision'
  AND (
      name = CAST('refs/heads/main' AS VARBINARY)
      OR name = CAST('refs/heads/master' AS VARBINARY)
  );

CREATE TABLE default.url_date_branch_2b AS
SELECT
    u.url,
    u.visit_date,
    sbf.revision_id
FROM default.url_date_snapshot_2a u
JOIN default.snapshot_branch_filtered sbf
    ON u.snapshot_id = sbf.snapshot_id;



