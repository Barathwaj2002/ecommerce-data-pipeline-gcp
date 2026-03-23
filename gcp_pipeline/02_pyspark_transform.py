from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, row_number, desc, round, count, coalesce, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import max as max_

BUCKET = "gs://ecommerce-pipeline-example"
INPUT_PATH = f"{BUCKET}/processed/cleaned_data.parquet"
OUTPUT_PATH_1 = f"{BUCKET}/processed/day4_ranked_products.parquet"
OUTPUT_PATH_2 = f"{BUCKET}/processed/customer_rfm.parquet"

def main():
    try:
        # Start SparkSession
        spark = SparkSession.builder.appName("Day4_Advanced").getOrCreate()
        df = spark.read.parquet(INPUT_PATH)
        print(f"Loaded {df.count()} rows and {len(df.columns)} columns")
        # Compute TotalPrice
        df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))
        rfm_df = df.groupBy("CustomerId", "Country") \
            .agg(
                sum_(col("Quantity") * col("UnitPrice")).alias("Monetary"),
                count("*").alias("Frequency"),
                max_("InvoiceDate").alias("LastPurchaseRecency")
            ) \
            .orderBy(col("Monetary").desc())
        # Customer tier data
        customer_data = [
            ("United Kingdom", "High"),
            ("Germany", "Medium"),
            ("France", "Low")
        ]
        customer_df = spark.createDataFrame(customer_data, ["Country", "CustomerTier"])

        # Join with customer tier
        joined_df = df.join(customer_df, "Country", "left")

        # print("=== Joined with Customer Tier ===")
        # joined_df.withColumn("TotalPrice", round(col("TotalPrice"), 2)).select(
        #     "CustomerID",
        #     "Country",
        #     "CustomerTier",
        #     "TotalPrice"
        # ).show(10)

        # Window function to rank top products per country
        window_spec = Window.partitionBy("Country").orderBy(desc("TotalRevenue"))

        ranked_df = (
            joined_df
            .groupBy("Country", "StockCode", "Description", "CustomerTier")
            .agg(round(sum_("TotalPrice"),2).alias("TotalRevenue"),
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


        # print("=== Top 5 Products per Country ===")
        # ranked_df.show(20, truncate=False)


        # Save result
        ranked_df.write \
            .mode("overwrite") \
            .option("overwriteSchema","true") \
            .parquet(OUTPUT_PATH_1)
        print(f"Saved ranked products to {OUTPUT_PATH_1}")
        rfm_df.write \
            .mode("overwrite") \
            .option("overwriteSchema","true") \
            .parquet(OUTPUT_PATH_2)
        print(f"Saved RFM data to {OUTPUT_PATH_2}")

        spark.stop()

    except Exception as e:
        # Raise exception so Airflow detects failure
        raise RuntimeError(f"PySpark transform failed: {e}") from e


if __name__ == "__main__":
    main()
