-- Step 6: Keep Latest Snapshot per Repository

CREATE TABLE default.filtered_github_unique AS
SELECT
    url,
    MAX_BY(content_sha1_git, visit_date) AS content_sha1_git,
    MAX(visit_date) AS visit_date
FROM default.filtered_github_total_table
GROUP BY url;
