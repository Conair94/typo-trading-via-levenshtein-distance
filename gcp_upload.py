import sqlite3
import pandas as pd
import sys

def upload_to_bigquery(db_path="typo_trading.db", project_id=None, dataset_id="typo_trading"):
    """
    Reads tables from SQLite and uploads to BigQuery.
    """
    try:
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
    except ImportError:
        print("Please install google-cloud-bigquery to use this feature.")
        print("pip install google-cloud-bigquery")
        return

    if not project_id:
        print("Please provide a Google Cloud Project ID.")
        return

    client = bigquery.Client(project=project_id)

    # specific functionality to create dataset if not exists
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} exists.")
    except NotFound:
        print(f"Dataset {dataset_id} not found. Creating...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Adjust as needed
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

    # Connect to SQLite
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
    except Exception as e:
        print(f"Error reading SQLite DB: {e}")
        return

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        print(f"Uploading table: {table_name}...")
        
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        
        # Load to BQ
        table_ref = dataset_ref.table(table_name)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", # Overwrite table
        )
        
        try:
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Waits for the job to complete.
            print(f"Loaded {job.output_rows} rows into {dataset_id}.{table_name}.")
        except Exception as e:
            print(f"Failed to upload {table_name}: {e}")

    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python gcp_upload.py <YOUR_GCP_PROJECT_ID>")
    else:
        upload_to_bigquery(project_id=sys.argv[1])
