"""Module 7: UDFs and Complex Types — Companion Script.

Run with: docker compose exec spark spark-submit scripts/07_udf_exercises.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list, size, struct, explode, pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

spark = SparkSession.builder \
    .appName("Script 07 - UDFs") \
    .master("local[*]") \
    .getOrCreate()

employees = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# UDF: categorize sale amounts
@udf(returnType=StringType())
def sale_size(amount):
    if amount is None:
        return None
    if amount >= 3000:
        return "Large"
    elif amount >= 1000:
        return "Medium"
    else:
        return "Small"

print("=== Sales Categorized by Size ===")
(
    sales
    .withColumn("sale_size", sale_size(col("amount")))
    .groupBy("sale_size")
    .count()
    .orderBy("count", ascending=False)
    .show()
)

# collect_list: products sold per employee
print("=== Products per Employee ===")
(
    sales
    .join(employees.select("employee_id", "name"), on="employee_id")
    .groupBy("name")
    .agg(
        collect_list("product").alias("products_sold"),
        size(collect_list("product")).alias("num_sales")
    )
    .orderBy(col("num_sales").desc())
    .show(10, truncate=False)
)

# Structs: nested employee info
print("=== Nested Employee Data ===")
nested = employees.select(
    "employee_id",
    struct("name", "city", "salary").alias("details")
)
nested.show(5, truncate=False)
nested.select("employee_id", "details.name", "details.salary").show(5)

# Pandas UDF: salary tax calculation
@pandas_udf("double")
def calc_tax(salary: pd.Series) -> pd.Series:
    return salary * 0.30

print("=== Salary with Tax (Pandas UDF) ===")
(
    employees
    .withColumn("tax", calc_tax(col("salary")))
    .select("name", "salary", "tax")
    .show(10)
)

spark.stop()
