"""Module 2: RDD Word Count — Companion Script.

Run with: docker compose exec spark spark-submit scripts/02_rdd_word_count.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Script 02 - RDD Word Count") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Classic word count
text = sc.parallelize([
    "the quick brown fox jumps over the lazy dog",
    "the fox was quick and the dog was lazy",
    "spark makes distributed computing easy",
    "spark is fast and spark is powerful",
])

word_counts = (
    text
    .flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
)

print("=== Word Counts ===")
for word, count in word_counts.collect():
    print(f"  {word}: {count}")

# Bonus: count cities from employees CSV using RDDs
print("\n=== City Counts from employees.csv ===")
lines = sc.textFile("data/employees.csv")
header = lines.first()
city_counts = (
    lines
    .filter(lambda line: line != header)
    .map(lambda line: line.split(","))
    .map(lambda fields: (fields[5], 1))  # city is the 6th column
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
)

for city, count in city_counts.collect():
    print(f"  {city}: {count}")

spark.stop()
