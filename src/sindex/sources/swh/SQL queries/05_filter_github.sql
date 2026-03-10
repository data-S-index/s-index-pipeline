-- Step 5: Filter GitHub Repositories Only
CREATE TABLE default.filtered_github_total_table AS
SELECT url, content_sha1_git, sha1, visit_date
FROM url_content_final
WHERE url LIKE 'https://github.com/%';
