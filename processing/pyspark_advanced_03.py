from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, row_number, desc, round, count, coalesce, lit, coalesce, lit
from pyspark.sql.window import Window
import os
import sys
import shutil
from pathlib import Path
import time
# --------------------------
# Base directory (ingestion/)
# --------------------------
BASE_DIR = Path(__file__).resolve().parent

# Paths
# --------------------------
# Paths (inside container)
# --------------------------
PROCESSED_DATA_PATH = Path("/app/ingestion/processed_data/cleaned_data.parquet")
RANKED_PRODUCTS_PATH = Path("/app/ingestion/processed_data/day4_ranked_products.parquet")


# --------------------------
# Set PySpark Python to current virtualenv Python
# --------------------------
# PYSPARK_PYTHON = f"{sys.prefix}/bin/python"
# os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
# os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON



# Wait up to 30s for the cleaned file
for _ in range(120):
    if Path(PROCESSED_DATA_PATH).exists():
        break
    time.sleep(1)
else:
    raise FileNotFoundError(f"Cleaned data not found at {PROCESSED_DATA_PATH}")

# --------------------------
# Main processing
# --------------------------
def main():
    try:
        # Start SparkSession
        spark = SparkSession.builder.appName("Day4_Advanced").master("local[*]").getOrCreate()

        # Load cleaned parquet data
        if not PROCESSED_DATA_PATH.exists():
            raise FileNotFoundError(f"Cleaned data not found at {PROCESSED_DATA_PATH}")
        
        df = spark.read.parquet(str(PROCESSED_DATA_PATH))

        # Compute TotalPrice
        df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

        # Customer tier data
        customer_data = [
            ("United Kingdom", "High"),
            ("Germany", "Medium"),
            ("France", "Low")
        ]
        customer_df = spark.createDataFrame(customer_data, ["Country", "CustomerTier"])

        # Join with customer tier
        joined_df = df.join(customer_df, "Country", "left")

        print("=== Joined with Customer Tier ===")
        joined_df.withColumn("TotalPrice", round(col("TotalPrice"), 2).alias("TotalPrice")).select(
            "CustomerID",
            "Country",
            "CustomerTier",
            "TotalPrice"
        ).show(10)

        # Window function to rank top products per country
        window_spec = Window.partitionBy("Country").orderBy(desc("TotalRevenue"))

        ranked_df = (
            joined_df
            .groupBy("Country", "StockCode", "Description", "CustomerTier")
            .agg(sum_("TotalPrice").alias("TotalRevenue"),
                 count("*").alias("OrderCount")
                 )
            .withColumn("Rank", row_number().over(window_spec))
            .filter(col("Rank") <= 5)
        )

        joined_df = df.join(customer_df, "Country", "left")
        joined_df = joined_df.withColumn(
            "CustomerTier",
            coalesce(col("CustomerTier"), lit("Unknown"))
        )


        print("=== Top 5 Products per Country ===")
        ranked_df.show(20, truncate=False)

        # Ensure output folder exists
        RANKED_PRODUCTS_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Save result
        ranked_df.write \
            .mode("overwrite") \
            .option("overwriteSchema","true") \
            .parquet(str(RANKED_PRODUCTS_PATH))
        print(f"Saved ranked products to {RANKED_PRODUCTS_PATH}")

        spark.stop()

    except Exception as e:
        # Raise exception so Airflow detects failure
        raise RuntimeError(f"PySpark transform failed: {e}") from e


if __name__ == "__main__":
    main()
