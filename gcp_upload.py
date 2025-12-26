import sqlite3
import pandas as pd
import sys
import os

def get_latest_data_dir(base_dir="data"):
    if not os.path.exists(base_dir):
        return None
    subdirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    if not subdirs:
        return None
    return max(subdirs, key=os.path.getmtime)

def upload_to_bigquery(db_path, project_id, dataset_id="typo_trading"):
    """
    Reads tables from SQLite and uploads to BigQuery.
    """
    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return

    try:
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
    except ImportError:
        print("Please install google-cloud-bigquery to use this feature.")
        print("pip install google-cloud-bigquery")
        return

    print(f"Uploading data from: {db_path}")
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
        print("Usage: python gcp_upload.py <YOUR_GCP_PROJECT_ID> [OPTIONAL_DB_PATH]")
        sys.exit(1)
    
    project_id = sys.argv[1]
    db_path = None
    
    # Check if DB path provided as arg
    if len(sys.argv) >= 3:
        db_path = sys.argv[2]
    else:
        # Find latest
        data_dir = get_latest_data_dir()
        if data_dir:
            potential_path = os.path.join(data_dir, "typo_trading.db")
            if os.path.exists(potential_path):
                db_path = potential_path
            else:
                print(f"No database found in latest directory: {data_dir}")
        else:
            print("No data directory found.")

    if db_path:
        upload_to_bigquery(db_path=db_path, project_id=project_id)
    else:
        print("Could not locate typo_trading.db. Please provide path explicitly.")