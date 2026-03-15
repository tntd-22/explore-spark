# Explore Apache Spark

A beginner-friendly Apache Spark learning workspace running entirely in Docker.

## Quick Start

```bash
docker compose up
```

Open **http://localhost:8888** in your browser (token: `spark`).

## What's Inside

Everything runs in a single Docker container (`quay.io/jupyter/all-spark-notebook`) which includes:
- PySpark + JupyterLab
- Spark shell (Scala)
- Java, Python, Scala pre-installed

## Curriculum

| # | Notebook | Topic |
|---|----------|-------|
| 1 | `01_spark_basics` | SparkSession, local mode, Spark UI |
| 2 | `02_rdds` | RDD creation, transformations, actions, word count |
| 3 | `03_dataframes_intro` | DataFrames, schemas, inspection |
| 4 | `04_dataframe_transforms` | select, filter, groupBy, agg, join |
| 5 | `05_spark_sql` | Temp views, SQL queries, explain plans |
| 6 | `06_reading_writing_data` | CSV, JSON, Parquet I/O, partitioning |
| 7 | `07_udfs_complex_types` | UDFs, arrays, maps, structs |
| 8 | `08_spark_scala_intro.md` | Scala guide for spark-shell |
| 9 | `09_spark_mllib` | MLlib: features, regression, classification, pipelines, tuning |

Start with notebook `01` and work through in order. Each builds on the previous.

## Running Scripts

```bash
# Run a companion Python script
docker compose exec spark spark-submit scripts/01_spark_basics.py

# Open Scala spark-shell
docker compose exec spark spark-shell
```

## Spark UI

While a SparkSession is active, visit **http://localhost:4040** to see jobs, stages, and execution plans.

## Sample Data

Three interrelated datasets in `data/`:
- `employees.csv` — 30 employees with salary, department, city
- `departments.csv` — 6 departments with budget
- `sales.csv` — 100 sales transactions

## Stopping

```bash
docker compose down
```

## Troubleshooting

- **Port 8888 in use**: Change the port mapping in `docker-compose.yml` (e.g., `9999:8888`)
- **Port 4040 in use**: Spark auto-increments to 4041, 4042, etc.
- **Only one SparkSession at a time**: Stop the kernel in one notebook before starting another
- **File not found errors**: Paths inside notebooks are relative to `/home/jovyan/` (e.g., `data/employees.csv`)
