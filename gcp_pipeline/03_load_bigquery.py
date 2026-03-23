from google.cloud import bigquery
import os

PROJECT_ID = "ecommerce-pipeline-490613"
DATASET_ID = "ecommerce_data"
BUCKET = "ecommerce-pipeline-example"

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

if __name__=="__main__":
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

    print("BigQuery load complete")
