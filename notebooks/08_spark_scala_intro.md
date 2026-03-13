# Module 8: Introduction to Spark with Scala

Spark is written in Scala, and many production Spark jobs use Scala. This guide introduces Scala Spark through the interactive `spark-shell`.

**Why learn Scala Spark?** Scala runs natively on the JVM — no serialization overhead between Python and Java. In performance-critical production jobs, Scala can be 2-10x faster than PySpark.

## Starting spark-shell

```bash
# From your host machine:
docker compose exec spark spark-shell
```

You'll see a Scala REPL with two pre-created variables:
- `spark` — the SparkSession
- `sc` — the SparkContext

Type `:quit` to exit.

---

## Concept 1: Quick Scala Syntax Primer

Scala is a statically-typed language on the JVM. You don't need to master it — just enough to read and write Spark code.

Key differences from Python:
- **`val`** = immutable (like Python without reassignment), **`var`** = mutable
- Types are inferred but can be declared explicitly
- Lambdas use `=>` instead of `lambda`
- `_` is a wildcard — shorthand for a single argument

```scala
// Variables (val = immutable, var = mutable)
val name = "Spark"      // type is inferred (String)
val x: Int = 42         // explicit type annotation

// String interpolation
println(s"Hello $name, x is $x")

// Lambda functions
val double = (x: Int) => x * 2
println(double(5))  // 10

// Collections
val nums = List(1, 2, 3, 4, 5)
val squared = nums.map(x => x * x)
val evens = nums.filter(_ % 2 == 0)  // _ is shorthand for the argument
```

#### Try It in spark-shell

1. Create a `val` called `greeting` with the value `"Hello Spark"` and print it
2. Create a list of numbers 1-10, use `.map` to triple each one, and use `.filter` to keep only values > 15
3. Print the result

```scala
// Your code:
val greeting = "Hello Spark"
println(greeting)

val nums = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
val result = nums.map(_ * 3).filter(_ > 15)
println(result)
// Expected: List(18, 21, 24, 27, 30)
```

---

## Concept 2: Reading CSV Files (DataFrames)

Just like PySpark, you read CSVs with `spark.read`. The API is nearly identical — the main difference is Scala syntax.

```scala
val employees = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/employees.csv")

employees.show()
employees.printSchema()
```

#### Try It in spark-shell

1. Read `data/departments.csv` with header and inferSchema
2. Print the schema and show all rows
3. Print the row count using `.count()`

```scala
// Your code:
val departments = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/departments.csv")

departments.printSchema()
departments.show()
println(s"Departments: ${departments.count()} rows")
```

---

## Concept 3: Select and Filter

Scala Spark uses `$"column"` as shorthand for `col("column")`. This is enabled by `import spark.implicits._` (which spark-shell imports automatically).

- `.as("alias")` instead of PySpark's `.alias("alias")`
- `$"salary" > 90000` instead of `col("salary") > 90000`

```scala
import spark.implicits._

// Select columns
employees.select("name", "salary").show(5)

// Filter
employees.filter($"salary" > 90000).show()

// Select with expressions
employees.select($"name", ($"salary" * 1.1).as("raised_salary")).show(5)
```

Note: `$"column"` is Scala shorthand for `col("column")`.

#### Try It in spark-shell

1. Select `name`, `city`, and `salary` from employees
2. Filter for employees in `"Chicago"` — use `$"city" === "Chicago"` (triple equals for equality!)
3. Add a computed column: `salary * 1.15` aliased as `"salary_with_raise"`

```scala
// Your code:
employees.select($"name", $"city", $"salary")
  .filter($"city" === "Chicago")
  .select($"name", $"salary", ($"salary" * 1.15).as("salary_with_raise"))
  .show()
```

---

## Concept 4: GroupBy, Aggregations, and Spark SQL

Aggregation functions are imported from `org.apache.spark.sql.functions._` — the underscore `_` imports everything (like Python's `*`).

Spark SQL works identically to PySpark — register a temp view and query with `spark.sql()`.

```scala
import org.apache.spark.sql.functions._

// GroupBy with aggregations
employees.groupBy("city").agg(
  count("*").as("num_employees"),
  round(avg("salary"), 2).as("avg_salary"),
  max("salary").as("max_salary")
).show()

// Spark SQL
employees.createOrReplaceTempView("employees")

spark.sql("""
  SELECT city, COUNT(*) as n, ROUND(AVG(salary), 2) as avg_sal
  FROM employees
  GROUP BY city
  ORDER BY avg_sal DESC
""").show()
```

#### Try It in spark-shell

1. Read `data/sales.csv` into a `val sales`
2. Group by `product` and compute: total revenue (`sum("amount")`), number of sales (`count("*")`)
3. Order by total revenue descending
4. Also try the SQL version: register sales as a temp view and write the equivalent query

```scala
// Your code:
val sales = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/sales.csv")

// DataFrame API
sales.groupBy("product").agg(
  round(sum("amount"), 2).as("total_revenue"),
  count("*").as("num_sales")
).orderBy($"total_revenue".desc).show()

// SQL version
sales.createOrReplaceTempView("sales")
spark.sql("""
  SELECT product, ROUND(SUM(amount), 2) AS total_revenue, COUNT(*) AS num_sales
  FROM sales
  GROUP BY product
  ORDER BY total_revenue DESC
""").show()
```

---

## Concept 5: Joins

Joins use `Seq("column")` for the join key (instead of PySpark's `on="column"`). The join type is the third argument.

```scala
val departments = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/departments.csv")

val empDept = employees.join(departments, Seq("department_id"), "inner")
empDept.select("name", "department_name", "salary").show(10)
```

#### Try It in spark-shell

1. Join `sales` to `employees` on `employee_id`
2. Then join the result to `departments` on `department_id`
3. Select `name`, `department_name`, `product`, `amount`
4. Show the top 10 rows ordered by `amount` descending

```scala
// Your code:
val fullData = sales
  .join(employees, Seq("employee_id"))
  .join(departments, Seq("department_id"))

fullData.select("name", "department_name", "product", "amount")
  .orderBy($"amount".desc)
  .show(10)
```

---

## Concept 6: RDD Operations

Scala RDDs are faster than PySpark RDDs because there's no Python-JVM serialization. The API is almost identical but uses Scala lambda syntax.

- `_.split(" ")` is shorthand for `x => x.split(" ")`
- `(_, 1)` creates a tuple `(word, 1)` — the underscore stands for the current element

```scala
// Create an RDD
val numbers = sc.parallelize(1 to 10)
val squares = numbers.map(x => x * x)
println(squares.collect().mkString(", "))

// Word count
val text = sc.parallelize(Seq("hello world", "hello spark", "spark is great"))
val wordCounts = text
  .flatMap(_.split(" "))
  .map((_, 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)

wordCounts.collect().foreach(println)
```

#### Try It in spark-shell

1. Create an RDD from `1 to 20`
2. Filter for even numbers only (`_ % 2 == 0`)
3. Map each to its square
4. Use `.reduce(_ + _)` to compute the sum of all squared evens
5. Print the result

```scala
// Your code:
val nums = sc.parallelize(1 to 20)
val result = nums
  .filter(_ % 2 == 0)
  .map(x => x * x)
  .reduce(_ + _)

println(s"Sum of squared evens: $result")
// Expected: 4 + 16 + 36 + 64 + 100 + 144 + 196 + 256 + 324 + 400 = 1540
```

---

## Key Differences: PySpark vs Scala

| Feature | PySpark | Scala |
|---------|---------|-------|
| Column reference | `col("name")` | `$"name"` or `col("name")` |
| Lambda | `lambda x: x * 2` | `x => x * 2` or `_ * 2` |
| Alias | `.alias("new_name")` | `.as("new_name")` |
| Imports | `from pyspark.sql.functions import *` | `import org.apache.spark.sql.functions._` |
| Null safety | Explicit `if x is None` | Built-in `Option` type |
| Performance | Slower (Python serialization) | Faster (runs natively on JVM) |
| Join syntax | `.join(df, on="col")` | `.join(df, Seq("col"))` |
| Equality filter | `col("x") == "value"` | `$"x" === "value"` (triple equals) |

---

## Writing Data

```scala
// Write as Parquet
sales.write.mode("overwrite").parquet("output/sales_scala_parquet")

// Write partitioned
sales.write.mode("overwrite").partitionBy("region").parquet("output/sales_scala_by_region")
```

---

## Capstone Exercise

Combine everything you've learned. In `spark-shell`:

1. Read all 3 CSV files (employees, departments, sales)
2. Join sales → employees → departments (3-table join)
3. Group by `department_name` and compute total revenue and number of sales
4. Order by total revenue descending
5. Write the result as Parquet to `output/dept_revenue_scala`
6. Also write a SQL version using CTEs

```scala
// --- Capstone Solution ---

// 1. Read all data
val employees = spark.read.option("header", "true").option("inferSchema", "true").csv("data/employees.csv")
val departments = spark.read.option("header", "true").option("inferSchema", "true").csv("data/departments.csv")
val sales = spark.read.option("header", "true").option("inferSchema", "true").csv("data/sales.csv")

// 2-4. DataFrame API version
import org.apache.spark.sql.functions._

val deptRevenue = sales
  .join(employees, Seq("employee_id"))
  .join(departments, Seq("department_id"))
  .groupBy("department_name")
  .agg(
    round(sum("amount"), 2).as("total_revenue"),
    count("*").as("num_sales")
  )
  .orderBy($"total_revenue".desc)

deptRevenue.show()

// 5. Write as Parquet
deptRevenue.write.mode("overwrite").parquet("output/dept_revenue_scala")

// 6. SQL version with CTE
employees.createOrReplaceTempView("employees")
departments.createOrReplaceTempView("departments")
sales.createOrReplaceTempView("sales")

spark.sql("""
  WITH full_data AS (
    SELECT d.department_name, s.amount
    FROM sales s
    JOIN employees e ON s.employee_id = e.employee_id
    JOIN departments d ON e.department_id = d.department_id
  )
  SELECT department_name,
         ROUND(SUM(amount), 2) AS total_revenue,
         COUNT(*) AS num_sales
  FROM full_data
  GROUP BY department_name
  ORDER BY total_revenue DESC
""").show()
```

---

## What You Learned

- **spark-shell** gives you an interactive Scala REPL with `spark` and `sc` pre-created
- **`$"column"`** is Scala shorthand for column references
- **`.as("alias")`** instead of PySpark's `.alias()`
- **`===`** for equality (not `==` like PySpark)
- **`Seq("col")`** for join keys
- **Scala lambdas** use `=>` — and `_` is shorthand for single arguments
- **RDDs in Scala** have no Python serialization overhead — faster than PySpark RDDs
- The **DataFrame API** is nearly identical between PySpark and Scala — easy to translate

## Next Steps

- **spark-submit with Scala**: Write `.scala` files, compile with `sbt`, submit with `spark-submit`
- **Case classes**: Use Scala case classes for type-safe DataFrames (`Dataset[T]`)
- **Spark Streaming**: Real-time data processing
- **MLlib**: Machine learning with Spark
