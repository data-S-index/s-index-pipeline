-- Step 4: Directory → README → Content SHA

CREATE TABLE default.directory_entry_readme AS
SELECT
    directory_id,
    target AS content_sha1_git
FROM swh_graph_2025_10_08.directory_entry
WHERE type = 'file'
  AND name IN (
      X'524541444D452E6D64',  -- README.md
      X'726561646D652E6D64',  -- readme.md
      X'524541444D45',        -- README
      X'524541444D452E747874' -- README.txt
  );

CREATE TABLE default.url_date_directory_sha_3b AS
SELECT
    u.url,
    u.visit_date,
    d.content_sha1_git
FROM default.url_date_rev_2c u
JOIN default.directory_entry_readme d
    ON u.directory_id = d.directory_id;


CREATE TABLE default.filtered_directory_sha1 AS
SELECT DISTINCT content_sha1_git
FROM default.url_date_directory_sha_3b;


CREATE TABLE default.content_matched AS
SELECT c.sha1_git, c.sha1
FROM swh_graph_2025_10_08.content c
JOIN default.filtered_directory_sha1 f
    ON c.sha1_git = f.content_sha1_git;


CREATE TABLE default.url_content_final AS
SELECT d.url, d.visit_date, d.content_sha1_git, cm.sha1
FROM default.url_date_directory_sha_3b d
JOIN default.content_matched cm
    ON d.content_sha1_git = cm.sha1_git;

