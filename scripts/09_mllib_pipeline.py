"""
Module 09 Companion Script: End-to-end MLlib Pipeline

Predicts sale amount using product, quantity, region, employee salary, and tenure.
Demonstrates Pipeline, CrossValidator, and model persistence.

Run with:
    docker compose exec spark spark-submit scripts/09_mllib_pipeline.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, lit
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ── Session ──────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("09 - MLlib Pipeline Script") \
    .master("local[*]") \
    .getOrCreate()

# ── Load data ────────────────────────────────────────────────────────
employees = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# ── Feature engineering ──────────────────────────────────────────────
combined = (
    sales.join(
        employees.select("employee_id", "salary", "city", "hire_date"),
        on="employee_id"
    )
    .withColumn("tenure_days", datediff(lit("2024-12-01"), col("hire_date")))
    .withColumnRenamed("amount", "label")
)

print(f"Total rows: {combined.count()}")
combined.show(5)

# ── Pipeline stages ──────────────────────────────────────────────────
product_indexer = StringIndexer(inputCol="product", outputCol="product_idx")
product_encoder = OneHotEncoder(inputCol="product_idx", outputCol="product_vec")

region_indexer = StringIndexer(inputCol="region", outputCol="region_idx")
region_encoder = OneHotEncoder(inputCol="region_idx", outputCol="region_vec")

city_indexer = StringIndexer(inputCol="city", outputCol="city_idx")
city_encoder = OneHotEncoder(inputCol="city_idx", outputCol="city_vec")

assembler = VectorAssembler(
    inputCols=["quantity", "salary", "tenure_days", "product_vec", "region_vec", "city_vec"],
    outputCol="features"
)

lr = LinearRegression(maxIter=20)

pipeline = Pipeline(stages=[
    product_indexer, product_encoder,
    region_indexer, region_encoder,
    city_indexer, city_encoder,
    assembler,
    lr
])

# ── Cross-validation ─────────────────────────────────────────────────
param_grid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.001, 0.01, 0.1, 1.0]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=RegressionEvaluator(metricName="rmse"),
    numFolds=3,
    seed=42
)

# ── Train ────────────────────────────────────────────────────────────
train, test = combined.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {train.count()}, Test: {test.count()}")

cv_model = cv.fit(train)

# ── Evaluate ─────────────────────────────────────────────────────────
predictions = cv_model.transform(test)
predictions.select("product", "region", "quantity", "label", "prediction").show(15)

rmse = RegressionEvaluator(metricName="rmse").evaluate(predictions)
r2 = RegressionEvaluator(metricName="r2").evaluate(predictions)

print(f"\n{'='*40}")
print(f"  Best RMSE: {rmse:.2f}")
print(f"  Best R²:   {r2:.4f}")
print(f"  Best CV RMSE: {min(cv_model.avgMetrics):.2f}")
print(f"{'='*40}")

# ── Save model ───────────────────────────────────────────────────────
model_path = "output/best_sales_model"
cv_model.bestModel.write().overwrite().save(model_path)
print(f"\nModel saved to {model_path}")

# ── Verify load ──────────────────────────────────────────────────────
loaded = PipelineModel.load(model_path)
print("Model loaded successfully!")

spark.stop()
