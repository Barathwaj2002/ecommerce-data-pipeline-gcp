from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.trigger_rule import TriggerRule
# from google.cloud import storage, bigquery
# import pandas as pd
# import io
# import subprocess


PROJECT_ID="ecommerce-pipeline-490613"
BUCKET="ecommerce-pipeline-example"
REGION="asia-south1"
CLUSTER_NAME = "ecommerce-cluster-{{ ds_nodash }}"
DATASET_ID = "ecommerce_data"


cluster_config={
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_size_gb": 30}
    },
    "worker_config": {
        "num_instances": 0
    },
    "software_config": {
        "image_version": "2.2-debian12"
    }
}
PYSPARK_JOB_CONFIG = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/scripts/02_pyspark_transform.py"
    }
}
def on_failure_callback(context):
    dag_id=context["dag"].dag_id
    task_id=context["task"].task_id
    logical_date=context["logical_date"]
    print(f"ALERT: Task {task_id} of DAG {dag_id} failed at {logical_date}")

default_args={
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': on_failure_callback
}

@dag(
    dag_id="gcp_etl_dag",
    start_date=datetime(2026, 3, 30),
    schedule="@daily",
    default_args=default_args,
    catchup=False
)
def gcp_etl_pipeline():
    @task
    def ingestion():
        from google.cloud import storage
        import pandas as pd
        import io
        RAW_PATH="raw/online_retail.csv"
        PROCESSED_PATH="processed/cleaned_data.parquet"

        def get_gcs_client():
            return storage.Client(project=PROJECT_ID)

        def read_csv_from_gcs(client, BUCKET, blob_path):
            print(f"Reading gcs://{BUCKET}/{blob_path}")
            bucket = client.bucket(BUCKET)
            blob = bucket.blob(blob_path)
            data  = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(data), encoding="unicode_escape")
            print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
            return df

        def clean_data(df):
            print(f"Cleaning data...")
            df = df.dropna(subset=['CustomerID', 'Description'])
            df = df[df['Quantity'] > 0]
            df = df[df['UnitPrice'] > 0]
            df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
            print(f"After cleaning: {len(df)} rows")
            return df

        def write_parquet_to_gcs(client, df, BUCKET, blob_path):
            print(f"Writing cleaned data to gcs://{BUCKET}/{blob_path}")
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
            buffer.seek(0)
            bucket = client.bucket(BUCKET)
            blob = bucket.blob(blob_path)
            blob.upload_from_file(buffer, content_type = 'application/octet-stream')
            print(f"Written {len(df)} rows to GCS")
        client = get_gcs_client()
        df = read_csv_from_gcs(client, BUCKET, RAW_PATH)
        df_clean = clean_data(df)
        write_parquet_to_gcs(client, df_clean, BUCKET, PROCESSED_PATH)
        print("Ingestion Done")
    
    cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config=cluster_config
    )

    # pyspark_job = pyspark_job

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark",
        job=PYSPARK_JOB_CONFIG,
        region=REGION,
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE
    )

    @task
    def load_to_bq():
        from google.cloud import bigquery
        def get_bq_client():
            return bigquery.Client(project=PROJECT_ID)

        def load_parquet_to_bq (client, gcs_uri, table_id):
            print(f"Loading {gcs_uri} into {table_id}")

            job_config = bigquery.LoadJobConfig(
                source_format = bigquery.SourceFormat.PARQUET,
                write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True
            )

            load_job = client.load_table_from_uri(
                gcs_uri,
                table_id,
                job_config = job_config
            )

            load_job.result()

            table = client.get_table(table_id)
            print(f"Loaded {table.num_rows} rows into {table_id}")

        client = get_bq_client()
        load_parquet_to_bq(
            client,
            gcs_uri=f"gs://{BUCKET}/processed/day4_ranked_products.parquet/*",
            table_id=f"{PROJECT_ID}.{DATASET_ID}.product_revenue"
        )

        load_parquet_to_bq(
            client,
            gcs_uri=f"gs://{BUCKET}/processed/customer_rfm.parquet/*",
            table_id = f"{PROJECT_ID}.{DATASET_ID}.customer_rfm"
        )
    
    # @task
    # def run_dbt():
    #     import subprocess
    #     dbt_dir=f"/home/airflow/gcs/dags/dbt"
    #     subprocess.run(
    #         ["dbt","build"],
    #         cwd=dbt_dir,
    #         check=True
    #     )
    ingest_task = ingestion()
    bq_task = load_to_bq()
    # dbt_task = run_dbt()

    ingest_task >> cluster >> submit_job >> delete_cluster >> bq_task 
    # >> dbt_task


dag = gcp_etl_pipeline()


