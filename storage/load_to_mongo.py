import os
from pymongo import MongoClient
import pandas as pd
import time

MONGO_URI = "mongodb://root:example@mongo:27017/"

# import socket, time
# for i in range(30):
#     try:
#         with socket.create_connection(("mongo.default.svc.cluster.local", 27017), timeout=2):
#             print("Mongo reachable")
#             break
#     except Exception:
#         print("Waiting for Mongo...")
#         time.sleep(2)
# else:
#     raise RuntimeError("Mongo not reachable")

client = MongoClient(MONGO_URI)
db = client["ecommerce"]

def load_parquet_to_mongo(file_path, collection_name):
    print(f"Loading {file_path} into {collection_name}...")
    df = pd.read_parquet(file_path)
    records = df.to_dict("records")
    collection = db[collection_name]
    if records:
        collection.insert_many(records)
        print(f"Inserted {len(records)} documents into {collection_name}")
    else:
        print("No data to insert")
    print(f"Total documents now: {collection.count_documents({})}")
    if collection.count_documents({}) > 0:
        print("Sample document:")
        print(collection.find_one())

if __name__ == "__main__":
    load_parquet_to_mongo(
        "/app/ingestion/processed_data/day4_ranked_products.parquet",
        "product_revenue"
    )
    time.sleep(100)
    client.close()
