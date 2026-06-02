import os

import duckdb


def create_datasets_db_from_ndjson(slim_folder: str, output_db_path: str):
    """
    Creates or updates a DuckDB file with a table 'my_datasets'.
    On first run, creates the table and indexes.
    On subsequent runs, inserts only new datasets (by dataset_id) with current timestamp.
    Contains dataset_id, id_type, pubyear, added_date (date dataset added to this DB).
    """
    all_files = [
        os.path.join(slim_folder, f)
        for f in os.listdir(slim_folder)
        if f.endswith(".ndjson")
    ]
    valid_files = [f for f in all_files if os.path.getsize(f) > 0]

    if not valid_files:
        print("No valid ndjson files found.")
        return

    with duckdb.connect(output_db_path) as con:
        # Create table if it doesn't exist yet
        con.execute("""
            CREATE TABLE IF NOT EXISTS my_datasets (
                dataset_id VARCHAR,
                id_type VARCHAR,
                pubyear INTEGER,
                added_date TIMESTAMP
            )
        """)

        print(f"Extracting data from {len(valid_files)} files")

        # Parse the NDJSONs into a temp table
        con.execute(
            """
            CREATE TEMP TABLE incoming AS
            WITH extracted_ids AS (
                SELECT 
                    (SELECT x FROM unnest(identifiers) AS t(x) 
                     WHERE x.identifier_type = 'doi' LIMIT 1) as doi_struct,
                    identifiers[1] as first_struct,
                    pubyear
                FROM read_json_auto(?, 
                    ignore_errors=True, 
                    columns={
                        'identifiers': 'STRUCT(identifier_type VARCHAR, identifier VARCHAR)[]',
                        'pubyear': 'INTEGER',
                    }
                )
            ),
            prioritized AS (
                SELECT 
                    COALESCE(doi_struct, first_struct) as best_struct,
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

        # Insert only datasets not already in the table
        con.execute("""
            INSERT INTO my_datasets
            SELECT 
                i.dataset_id,
                i.id_type,
                i.pubyear,
                CURRENT_TIMESTAMP AS added_date
            FROM incoming i
            WHERE NOT EXISTS (
                SELECT 1 FROM my_datasets d 
                WHERE d.dataset_id = i.dataset_id
            )
        """)

        new_count = con.execute("""
            SELECT count(*) FROM incoming i
            WHERE NOT EXISTS (
                SELECT 1 FROM my_datasets d 
                WHERE d.dataset_id = i.dataset_id
            )
        """).fetchone()[0]

        # Create index if it doesn't exist
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_dataset_id 
            ON my_datasets (dataset_id)
        """)

        con.execute("CHECKPOINT;")
        total_count = con.execute("SELECT count(*) FROM my_datasets").fetchone()[0]

    print(f"\nSuccess! DB at: {output_db_path}")
    print(f"New datasets added: {new_count:,}")
    print(f"Total datasets in DB: {total_count:,}")
