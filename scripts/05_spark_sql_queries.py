"""Module 5: Spark SQL Queries — Companion Script.

Run with: docker compose exec spark spark-submit scripts/05_spark_sql_queries.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Script 05 - Spark SQL") \
    .master("local[*]") \
    .getOrCreate()

employees = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
departments = spark.read.csv("data/departments.csv", header=True, inferSchema=True)
sales = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

employees.createOrReplaceTempView("employees")
departments.createOrReplaceTempView("departments")
sales.createOrReplaceTempView("sales")

# Query 1: High earners by city
print("=== High Earners by City ===")
spark.sql("""
    SELECT city, COUNT(*) AS high_earners, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    WHERE salary > 80000
    GROUP BY city
    ORDER BY high_earners DESC
""").show()

# Query 2: Department revenue
print("=== Department Revenue ===")
spark.sql("""
    SELECT d.department_name,
           ROUND(SUM(s.amount), 2) AS total_revenue,
           COUNT(*) AS num_sales
    FROM sales s
    JOIN employees e ON s.employee_id = e.employee_id
    JOIN departments d ON e.department_id = d.department_id
    GROUP BY d.department_name
    ORDER BY total_revenue DESC
""").show()

# Query 3: Monthly sales trend
print("=== Monthly Sales Trend ===")
spark.sql("""
    SELECT SUBSTRING(sale_date, 1, 7) AS month,
           ROUND(SUM(amount), 2) AS total_revenue,
           COUNT(*) AS num_sales
    FROM sales
    GROUP BY SUBSTRING(sale_date, 1, 7)
    ORDER BY month
""").show()

# Query 4: Top products per region (using window function)
print("=== Top Product per Region ===")
spark.sql("""
    WITH product_revenue AS (
        SELECT region, product,
               ROUND(SUM(amount), 2) AS revenue,
               ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) AS rank
        FROM sales
        GROUP BY region, product
    )
    SELECT region, product, revenue
    FROM product_revenue
    WHERE rank = 1
    ORDER BY revenue DESC
""").show()

# Query 5: Employees ranked by salary within department
print("=== Salary Rank within Department ===")
spark.sql("""
    SELECT e.name, d.department_name, e.salary,
           RANK() OVER (PARTITION BY d.department_name ORDER BY e.salary DESC) AS salary_rank
    FROM employees e
    JOIN departments d ON e.department_id = d.department_id
    ORDER BY d.department_name, salary_rank
""").show(20)

spark.stop()
