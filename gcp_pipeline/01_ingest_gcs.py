from google.cloud import storage
import pandas as pd
import io

PROJECT_ID="ecommerce-pipeline-490613"
BUCKET_NAME="ecommerce-pipeline-example"
RAW_PATH="raw/online_retail.csv"
PROCESSED_PATH="processed/cleaned_data.parquet"

def get_gcs_client():
    return storage.Client(project=PROJECT_ID)

def read_csv_from_gcs(client, bucket_name, blob_path):
    print(f"Reading gcs://{bucket_name}/{blob_path}")
    bucket = client.bucket(bucket_name)
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

def write_parquet_to_gcs(client, df, bucket_name, blob_path):
    print(f"Writing cleaned data to gcs://{bucket_name}/{blob_path}")
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
    buffer.seek(0)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_file(buffer, content_type = 'application/octet-stream')
    print(f"Written {len(df)} rows to GCS")

if __name__ == "__main__":
    client = get_gcs_client()
    df = read_csv_from_gcs(client, BUCKET_NAME, RAW_PATH)
    df_clean = clean_data(df)
    write_parquet_to_gcs(client, df_clean, BUCKET_NAME, PROCESSED_PATH)
    print("Ingestion Done")


