import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sum_, count, max as max_, col

# --------------------------
# Start Spark session
# --------------------------
spark = SparkSession.builder.appName("GenerateRFM").getOrCreate()

# --------------------------
# Paths inside container
# --------------------------
PROCESSED_DATA_PATH = Path("/app/ingestion/processed_data/cleaned_data.parquet")
CUSTOMER_RFM_PATH = Path("/app/ingestion/processed_data/customer_rfm.parquet")

# Wait for cleaned data (optional if job dependencies are guaranteed)
if not PROCESSED_DATA_PATH.exists():
    raise FileNotFoundError(f"Cleaned data not found at {PROCESSED_DATA_PATH}")

# --------------------------
# Load cleaned parquet
# --------------------------
df = spark.read.parquet(str(PROCESSED_DATA_PATH))

# --------------------------
# Calculate RFM metrics
# --------------------------
rfm_df = df.groupBy("CustomerId", "Country") \
    .agg(
        sum_(col("Quantity") * col("UnitPrice")).alias("Monetary"),
        count("*").alias("Frequency"),
        max_("InvoiceDate").alias("LastPurchaseRecency")
    ) \
    .orderBy(col("Monetary").desc())

# --------------------------
# Ensure output folder exists
# --------------------------
CUSTOMER_RFM_PATH.parent.mkdir(parents=True, exist_ok=True)

# --------------------------
# Save as parquet (Spark handles overwrite)
# --------------------------
rfm_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .parquet(str(CUSTOMER_RFM_PATH))

print(f"RFM data saved to {CUSTOMER_RFM_PATH}")
rfm_df.show(10)

# --------------------------
# Stop Spark
# --------------------------
spark.stop()