"""Module 3: DataFrame Exploration — Companion Script.

Run with: docker compose exec spark spark-submit scripts/03_dataframe_explore.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Script 03 - DataFrame Explore") \
    .master("local[*]") \
    .getOrCreate()

# Read all three datasets
employees = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
departments = spark.read.csv("data/departments.csv", header=True, inferSchema=True)
sales = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

for name, df in [("Employees", employees), ("Departments", departments), ("Sales", sales)]:
    print(f"\n{'='*50}")
    print(f"{name}: {df.count()} rows, {len(df.columns)} columns")
    print(f"Columns: {df.columns}")
    print(f"{'='*50}")
    df.printSchema()
    df.show(5)

print("\n=== Employee Salary Statistics ===")
employees.describe("salary").show()

print("\n=== Sales Amount Statistics ===")
sales.describe("amount", "quantity").show()

spark.stop()
