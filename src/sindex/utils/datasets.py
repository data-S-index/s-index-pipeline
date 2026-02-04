import os

import duckdb


def create_datasets_db_from_ndjson(slim_folder: str, output_db_path: str):
    """
    Creates a DuckDB file with a table 'my_datasets' and a fast search index.
    Contains dataset_id, id_type,
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
        print(f"Extracting data from {len(valid_files)} files")

        setup_query = """
        CREATE OR REPLACE TABLE my_datasets AS
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
        WHERE dataset_id IS NOT NULL;
        """
        con.execute(setup_query, [valid_files])

        # Add the Index
        print("Creating index for fast lookups (this may take a moment)")
        con.execute("CREATE INDEX idx_dataset_id ON my_datasets (dataset_id);")

        # Finalize
        print("Saving to disk")
        con.execute("CHECKPOINT;")

        count = con.execute("SELECT count(*) FROM my_datasets").fetchone()[0]

    print(f"\nSuccess! Persistent DB created at: {output_db_path}")
    print(f"Total indexed datasets: {count:,}")
