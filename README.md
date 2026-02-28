# 🌐 DataWarehouse ETL Suite — Batch Data Processing with Apache Spark

[![Scala](https://img.shields.io/badge/Scala-2.13-DC322F?style=for-the-badge&logo=scala&logoColor=white)](https://www.scala-lang.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

> An automated ETL (Extract, Transform, Load) suite for ingesting structured data from multiple sources, applying complex transformations using Apache Spark, and loading clean data into a PostgreSQL data warehouse for analytics.

---

## 📋 Table of Contents
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [ETL Pipeline](#etl-pipeline)
- [Project Structure](#project-structure)

---

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        ETL PIPELINE ARCHITECTURE                       │
│                                                                        │
│  EXTRACT                  TRANSFORM                    LOAD            │
│  ┌────────────┐          ┌──────────────────────┐    ┌─────────────┐  │
│  │ CSV Files  │──────┐   │  Apache Spark         │    │             │  │
│  │ JSON Files │──────┼──▶│  ┌────────────────┐  │───▶│ PostgreSQL  │  │
│  │ DB Sources │──────┘   │  │ Validate Schema │  │    │ Data        │  │
│  └────────────┘          │  │ Clean Nulls     │  │    │ Warehouse   │  │
│                          │  │ Type Casting    │  │    │             │  │
│  STAGING AREA            │  │ Deduplication   │  │    └─────────────┘  │
│  ┌────────────┐          │  │ Aggregation     │  │                     │
│  │ Raw Data   │          │  │ Enrichment      │  │    MONITORING       │
│  │ Landing    │          │  └────────────────┘  │    ┌─────────────┐  │
│  │ Zone       │          └──────────────────────┘    │ Pipeline    │  │
│  └────────────┘                                       │ Logs & Audit│  │
│                                                        └─────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

- 📥 **Multi-Source Extraction** — CSV, JSON, JDBC database sources
- 🧹 **Data Cleansing** — Null handling, type coercion, deduplication
- 🔄 **Schema Validation** — Enforce schema contracts at ingestion
- ⚡ **Spark Batch Processing** — Distributed processing for large datasets
- 🏗️ **Star Schema** — Dimension and fact table loading into data warehouse
- 📊 **Aggregations** — Pre-compute metrics for analytics dashboards
- 📝 **Audit Logging** — Track rows processed, rejected, and loaded
- 🐳 **Dockerized** — Fully containerized for easy deployment
- ♻️ **Idempotent Loads** — Safe to re-run without duplicating data

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Processing Engine | Apache Spark 3.x |
| Language | Scala 2.13 |
| Data Warehouse | PostgreSQL 15 |
| Build Tool | SBT |
| Container | Docker + Docker Compose |
| Scheduling (optional) | Apache Airflow / cron |

---

## 🚀 Getting Started

### Prerequisites
- Java 11+, Scala 2.13, SBT, Docker

```bash
git clone https://github.com/Amanrai1480/DataWarehouse-ETL.git
cd DataWarehouse-ETL

# Start PostgreSQL
docker-compose up -d postgres

# Build
sbt assembly

# Run ETL pipeline
spark-submit \
  --class com.dwetl.MainPipeline \
  --master local[*] \
  --driver-memory 2g \
  target/scala-2.13/dw-etl-assembly-1.0.jar \
  --config config/pipeline.conf
```

---

## 🔄 ETL Pipeline

### Extract Stage
```scala
object Extractor {

  def extractFromCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .csv(path)
  }

  def extractFromJDBC(spark: SparkSession, config: DbConfig): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", config.url)
      .option("dbtable", config.table)
      .option("user", config.user)
      .option("password", config.password)
      .option("numPartitions", "8")
      .load()
  }
}
```

### Transform Stage
```scala
object Transformer {

  def cleanAndTransform(df: DataFrame): DataFrame = {
    df
      // Drop rows where critical fields are null
      .na.drop(Seq("id", "created_at"))
      // Fill optional nulls with defaults
      .na.fill(Map("status" -> "UNKNOWN", "amount" -> 0.0))
      // Standardize types
      .withColumn("amount", col("amount").cast(DoubleType))
      .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))
      // Deduplicate on primary key, keep latest
      .dropDuplicates(Seq("id"))
      // Derived columns
      .withColumn("year", year(col("created_at")))
      .withColumn("month", month(col("created_at")))
  }

  def aggregateDailyMetrics(df: DataFrame): DataFrame = {
    df.groupBy(col("year"), col("month"), col("category"))
      .agg(
        count("id").as("total_records"),
        sum("amount").as("total_amount"),
        avg("amount").as("avg_amount"),
        max("created_at").as("last_updated")
      )
  }
}
```

### Load Stage
```scala
object Loader {

  def loadToPostgres(df: DataFrame, table: String, mode: String = "append"): Unit = {
    df.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/datawarehouse")
      .option("dbtable", table)
      .option("user", "dw_user")
      .option("password", "password")
      .option("batchsize", "10000")
      .mode(mode)
      .save()

    println(s"[LOAD] Successfully loaded ${df.count()} rows into $table")
  }
}
```

---

## 📁 Project Structure

```
DataWarehouse-ETL/
├── src/main/scala/com/dwetl/
│   ├── MainPipeline.scala          # Entry point, orchestration
│   ├── extract/
│   │   ├── Extractor.scala         # CSV, JSON, JDBC extractors
│   │   └── SourceConfig.scala
│   ├── transform/
│   │   ├── Transformer.scala       # Cleansing, type casting
│   │   ├── Validator.scala         # Schema validation
│   │   └── Aggregator.scala        # Metrics aggregation
│   ├── load/
│   │   ├── Loader.scala            # JDBC sink to PostgreSQL
│   │   └── AuditLogger.scala       # Track pipeline runs
│   └── utils/
│       ├── SparkUtils.scala
│       └── ConfigLoader.scala
│
├── config/
│   └── pipeline.conf               # Source/target config
├── data/
│   └── sample/                     # Sample input files
├── sql/
│   └── create_warehouse.sql        # DDL for DW tables
├── docker-compose.yml
├── build.sbt
└── README.md
```

---

## 🗄️ Data Warehouse Schema (Star Schema)

```sql
-- Dimension Tables
dim_date      (date_id, date, year, month, day, quarter, day_of_week)
dim_category  (category_id, name, parent_category, description)
dim_user      (user_id, name, email, region, segment)

-- Fact Table
fact_transactions (
    transaction_id  BIGINT PRIMARY KEY,
    date_id         INT REFERENCES dim_date,
    category_id     INT REFERENCES dim_category,
    user_id         INT REFERENCES dim_user,
    amount          NUMERIC(12,2),
    status          VARCHAR(50),
    created_at      TIMESTAMP
)

-- Aggregation Table (pre-computed)
agg_daily_metrics (
    id              BIGSERIAL PRIMARY KEY,
    year            INT,
    month           INT,
    category        VARCHAR(100),
    total_records   BIGINT,
    total_amount    NUMERIC(15,2),
    avg_amount      NUMERIC(10,2),
    pipeline_run_id VARCHAR(50),
    loaded_at       TIMESTAMP DEFAULT NOW()
)

-- Audit Log
pipeline_runs (
    run_id          VARCHAR(50) PRIMARY KEY,
    pipeline_name   VARCHAR(100),
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    rows_extracted  BIGINT,
    rows_rejected   BIGINT,
    rows_loaded     BIGINT,
    status          VARCHAR(20)
)
```

---

## 📄 License
[MIT](LICENSE)
