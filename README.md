# module2_assignment_project



### CSV -> DuckDB -> Parquet -> BigQuery

1. Run the duckdb_ingest.py file

    ```python duckdb_ingest.py```

2. Execute codes in `project_quickstart.ipynb`

3. Run the export_parquet.py file
    ```python export_parquet.py```

4. To Load parquet files to BigQuery Tables, run this code in command line.
    ```
    for f in parquet/*.parquet; do
    tbl=$(basename "$f" .parquet)   # e.g., customers.parquet -> customers
    echo "Loading $f -> olist_raw2.$tbl ..."
    bq --location=asia-southeast1 load \
        --autodetect --replace \
        --source_format=PARQUET \
        module2-assignment-project:olist_raw2.$tbl "$f"
    done
    ```