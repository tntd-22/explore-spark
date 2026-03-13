"""Module 6: Data I/O — Companion Script.

Reads CSV, writes all formats, compares sizes.
Run with: docker compose exec spark spark-submit scripts/06_data_io.py
"""
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Script 06 - Data IO") \
    .master("local[*]") \
    .getOrCreate()

sales = spark.read.csv("data/sales.csv", header=True, inferSchema=True)
print(f"Loaded sales: {sales.count()} rows")

# Write in all formats
sales.write.mode("overwrite").csv("output/sales_csv", header=True)
sales.write.mode("overwrite").json("output/sales_json")
sales.write.mode("overwrite").parquet("output/sales_parquet")
sales.write.mode("overwrite").partitionBy("region").parquet("output/sales_by_region")

print("Written CSV, JSON, Parquet, and partitioned Parquet.")

# Read back and verify
for fmt in ["csv", "json", "parquet"]:
    path = f"output/sales_{fmt}"
    if fmt == "csv":
        df = spark.read.csv(path, header=True, inferSchema=True)
    elif fmt == "json":
        df = spark.read.json(path)
    else:
        df = spark.read.parquet(path)
    print(f"\n{fmt.upper()}: {df.count()} rows, {len(df.columns)} columns")
    df.printSchema()

# Compare file sizes
def dir_size(path):
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            total += os.path.getsize(os.path.join(dirpath, f))
    return total

print("\n=== File Size Comparison ===")
for name, path in [("CSV", "output/sales_csv"), ("JSON", "output/sales_json"), ("Parquet", "output/sales_parquet")]:
    size = dir_size(path)
    print(f"  {name:>8}: {size:>8,} bytes")

# Show partition structure
print("\n=== Partition Structure ===")
for item in sorted(os.listdir("output/sales_by_region")):
    if item.startswith("region="):
        print(f"  {item}")

spark.stop()
