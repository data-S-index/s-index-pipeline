import os
from datetime import datetime

import duckdb

VALID_ID_TYPES = {"doi", "emdb_id"}


def add_datasets_db_from_ndjson(
    slim_folder: str,
    output_db_path: str,
    id_type: str = "doi",
    added_date: datetime | None = None,
):
    """
    Creates or updates a DuckDB file with a table 'my_datasets'.
    On first run, creates the table and indexes.
    On subsequent runs, inserts only new datasets (by dataset_id) with current timestamp.
    Contains dataset_id, id_type, pubyear, added_date (date dataset added to this DB).

    Args:
        slim_folder: Folder containing .ndjson files.
        output_db_path: Path to DuckDB file (created if it doesn't exist).
        id_type: Identifier type to prefer, e.g. 'doi' or 'emdb_id'.
        added_date: Timestamp to use for added_date. In format "YYYY-MM-DD". Defaults to current timestamp.
    """
    if id_type not in VALID_ID_TYPES:
        raise ValueError(f"id_type must be one of {VALID_ID_TYPES}, got {id_type!r}")

    all_files = [
        os.path.join(slim_folder, f)
        for f in os.listdir(slim_folder)
        if f.endswith(".ndjson")
    ]
    valid_files = [f for f in all_files if os.path.getsize(f) > 0]

    if not valid_files:
        print("No valid ndjson files found.")
        return

    timestamp = (
        datetime.strptime(added_date, "%Y-%m-%d")
        if isinstance(added_date, str)
        else (added_date if added_date is not None else datetime.now())
    )

    with duckdb.connect(output_db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS my_datasets (
                dataset_id VARCHAR,
                id_type VARCHAR,
                pubyear INTEGER,
                added_date TIMESTAMP
            )
        """)

        print(f"Extracting data from {len(valid_files)} files")

        con.execute(
            f"""
            CREATE TEMP TABLE incoming AS
            WITH extracted_ids AS (
                SELECT
                    (SELECT x FROM unnest(identifiers) AS t(x)
                     WHERE x.identifier_type = '{id_type}' LIMIT 1) AS preferred_struct,
                    identifiers[1] AS first_struct,
                    pubyear
                FROM read_json_auto(?,
                    ignore_errors=True,
                    columns={{
                        'identifiers': 'STRUCT(identifier_type VARCHAR, identifier VARCHAR)[]',
                        'pubyear': 'INTEGER'
                    }}
                )
            ),
            prioritized AS (
                SELECT
                    COALESCE(preferred_struct, first_struct) AS best_struct,
                    pubyear
                FROM extracted_ids
            )
            SELECT
                best_struct.identifier AS dataset_id,
                best_struct.identifier_type AS id_type,
                pubyear
            FROM prioritized
            WHERE dataset_id IS NOT NULL
            """,
            [valid_files],
        )

        new_count = con.execute("""
            SELECT count(*) FROM incoming i
            WHERE NOT EXISTS (
                SELECT 1 FROM my_datasets d
                WHERE d.dataset_id = i.dataset_id
            )
        """).fetchone()[0]

        con.execute(
            """
            INSERT INTO my_datasets
            SELECT
                i.dataset_id,
                i.id_type,
                i.pubyear,
                ? AS added_date
            FROM incoming i
            WHERE NOT EXISTS (
                SELECT 1 FROM my_datasets d
                WHERE d.dataset_id = i.dataset_id
            )
            """,
            [timestamp],
        )

        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_dataset_id
            ON my_datasets (dataset_id)
        """)

        con.execute("CHECKPOINT;")
        total_count = con.execute("SELECT count(*) FROM my_datasets").fetchone()[0]

    print(f"\nSuccess! DB at: {output_db_path}")
    print(f"New datasets added: {new_count:,}")
    print(f"Total datasets in DB: {total_count:,}")
