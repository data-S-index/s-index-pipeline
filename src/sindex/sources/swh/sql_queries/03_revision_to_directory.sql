-- Step 3: Revision → Root Directory
CREATE TABLE default.url_date_rev_2c AS
SELECT
    b.url,
    b.visit_date,
    r.directory as directory_id
FROM default.url_date_branch_2b b
JOIN swh_graph_2025_10_08.revision r
    ON b.revision_id = r.id;


