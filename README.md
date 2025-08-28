# Module2 Assignment Project
## Olist Dataset

### Step 1: Ingestion
#### CSV -> DuckDB -> Parquet -> BigQuery

1. Run the duckdb_ingest.py file to create the olist.duckdb database: 
    ```python duckdb_ingest.py```

2. Quick exploration of the DuckDB database (Step 1), execute codes in `project_quickstart.ipynb`

3. To export all tables from the DuckDB database (Step 1) to individual Parquet files, run the export_parquet.py: 
    ```python export_parquet.py```

4. To load the parquet files (from Step 3) to BigQuery Tables, `cd` to the folder that contains the /parquet folder. Copy-paste and run this code in terminal:
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
_______________________________________________________
### Step 2: ??