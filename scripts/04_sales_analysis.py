"""Module 4: Sales Analysis — Companion Script.

Demonstrates multi-table joins, groupBy, aggregations.
Run with: docker compose exec spark spark-submit scripts/04_sales_analysis.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round, max

spark = SparkSession.builder \
    .appName("Script 04 - Sales Analysis") \
    .master("local[*]") \
    .getOrCreate()

employees = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
departments = spark.read.csv("data/departments.csv", header=True, inferSchema=True)
sales = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# Revenue by department
print("=== Revenue by Department ===")
(
    sales
    .join(employees, on="employee_id")
    .join(departments, on="department_id")
    .groupBy("department_name")
    .agg(
        round(sum("amount"), 2).alias("total_revenue"),
        count("*").alias("num_sales"),
        round(avg("amount"), 2).alias("avg_sale")
    )
    .orderBy(col("total_revenue").desc())
    .show()
)

# Top 10 sellers
print("=== Top 10 Sellers ===")
(
    sales
    .join(employees, on="employee_id")
    .groupBy("name")
    .agg(
        round(sum("amount"), 2).alias("total_sales"),
        count("*").alias("num_transactions")
    )
    .orderBy(col("total_sales").desc())
    .show(10)
)

# Revenue by region
print("=== Revenue by Region ===")
(
    sales
    .groupBy("region")
    .agg(
        round(sum("amount"), 2).alias("total_revenue"),
        count("*").alias("num_sales")
    )
    .orderBy(col("total_revenue").desc())
    .show()
)

# Revenue by product
print("=== Revenue by Product ===")
(
    sales
    .groupBy("product")
    .agg(
        round(sum("amount"), 2).alias("total_revenue"),
        sum("quantity").alias("total_units")
    )
    .orderBy(col("total_revenue").desc())
    .show()
)

spark.stop()
