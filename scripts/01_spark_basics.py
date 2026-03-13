"""Module 1: Spark Basics — Companion Script.

Run with: docker compose exec spark spark-submit scripts/01_spark_basics.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Script 01 - Spark Basics") \
    .master("local[*]") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

# Create a simple DataFrame
data = [
    ("Alice", 30, "San Francisco"),
    ("Bob", 25, "New York"),
    ("Carol", 35, "Chicago"),
]
df = spark.createDataFrame(data, ["name", "age", "city"])
df.show()
df.printSchema()

# Read employees CSV
employees = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
print(f"Employees: {employees.count()} rows, {len(employees.columns)} columns")
employees.show(5)

spark.stop()
print("Done!")
