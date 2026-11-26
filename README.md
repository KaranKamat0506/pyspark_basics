# 🔥 PySpark Basics & Practice

> A comprehensive, hands-on collection of **120 PySpark scripts**, practice questions, and interview prep — built from the ground up while learning Apache Spark with a focus on Data Engineering workflows.

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange?logo=apachespark&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange?logo=jupyter&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)
![Scripts](https://img.shields.io/badge/Scripts-120-blueviolet)

---

## 📖 About

This repository documents my journey learning **Apache Spark** through PySpark. It covers core concepts, file format operations, advanced transformations, and curated practice questions — including a structured **100-problem curriculum** (7 modules completed so far) and a dedicated section for **interview-style problems**. All code is written in Python and tested in a local Spark environment.

---

## 📁 Repository Structure

```
pyspark_basics/
│
├── pyspark.ipynb                    # Core PySpark notebook — fundamentals & experiments
│
├── pyspark_100/                     # 📚 Structured 100-problem curriculum (7 modules)
│   ├── module_info.md               #    Complete roadmap & module descriptions
│   ├── Module 1 — Fundamentals/     #    10 exercises: DataFrames, schemas, columns
│   ├── Module 2 — Filtering and Sorting/  # 10 exercises: filter, sort, distinct
│   ├── Module 3 — Aggregation/      #    10 exercises: groupBy, agg, having
│   ├── Module 4 — Joins/            #    9 exercises: all join types
│   ├── Module 5 — Conditions, Strings, Dates and NULLs/  # 11 exercises
│   ├── Module 6 — Window Functions/  #   15 exercises: rank, lag, lead, running totals
│   ├── Module 7 — Working with Nested Data/  # 10 exercises: arrays, maps, structs, JSON
│   └── revision/                    #    Revision scripts for modules 1–3, 1–6, and 7
│
├── joinsInPyspark/                  # 🔗 Deep-dive into PySpark joins
│   ├── Basic Joins.py               #    inner, left, left_anti joins
│   ├── compare_source_and_target.py #    Insert/Update/Delete detection via joins
│   ├── duplicateJoins.py            #    Handling duplicates with window + join
│   └── sem_anti_join.py             #    Semi & anti join patterns
│
├── pyspark_interview/               # 🎯 Interview-style questions with explanations
│   ├── collect_list.py              #    collect_list + concat_ws aggregation
│   ├── dropduplicate.py             #    Deduplication strategies
│   ├── dropMalformed.py             #    Handling malformed CSV data
│   ├── highestRankEmp.py            #    Highest salary per dept (dense_rank vs row_number)
│   ├── secondRankEmp.py             #    Second-highest salary per dept
│   ├── repartition_coalesce.py      #    Repartition vs coalesce deep-dive
│   ├── *_explanation.md             #    Detailed explanations for key problems
│   └── dropMalformed.csv            #    Sample data for malformed records exercise
│
├── pyspark_questions/               # 🧩 Practice problems covering real-world scenarios
│   ├── pysparkq1.py                 #    Conditional columns with when/otherwise
│   ├── pysparkq2.py                 #    DataFrame transformations
│   ├── pysparkq3.py                 #    DataFrame operations
│   ├── pyspark4–8.py                #    Train schedule (lead), ranking, window functions
│   ├── monthlySalesSummaryReport.py #    End-to-end monthly sales aggregation report
│   ├── test.py                      #    Test scripts
│   ├── lead_lag/                    #    Lead & lag window function exercises
│   └── sales.csv                    #    Sample sales dataset
│
├── parquetFileOperations/           # 📦 Reading, writing & querying Parquet files
│   ├── readFromParquetFile.py       #    Write & read Parquet files
│   ├── predicatePushdown.py         #    Predicate pushdown optimization + explain()
│   └── people_parquet/              #    Sample Parquet dataset
│
├── partitioning_bucketing/          # 📊 Partitioning & bucketing strategies
│   └── partitioningAndBucketing.py  #    partitionBy, bucketBy, sortBy, saveAsTable
│
├── readFromJson/                    # 📄 Loading & processing JSON data
│   ├── readFromJson.py              #    Basic JSON read with schema inference
│   ├── readJsonArrayExplode.py      #    Multiline JSON + explode arrays
│   ├── data.json                    #    Sample flat JSON
│   └── multilineJson.json           #    Sample nested/multiline JSON
│
├── SCD_TYPE2/                       # 🔄 Slowly Changing Dimension Type 2
│   └── scd_type_2.py               #    Full SCD2 implementation (detect, expire, insert)
│
└── windowFunctions/                 # 🪟 Standalone window function examples
    └── secondHighestSalary.py       #    Second-highest salary using dense_rank
```

---

## 🧠 Topics Covered

### Core DataFrame Operations
- **SparkSession & SparkContext** — setting up and configuring Spark applications
- **DataFrames** — creating from Python collections, transforming, filtering, aggregating
- **Schema Definition** — using `StructType`, `StructField` for explicit schemas
- **Column Operations** — `withColumn`, `withColumnRenamed`, `drop`, `select`, `cast`

### Filtering, Sorting & Deduplication
- **Filtering** — `filter()`, `where()`, single/multi-condition, `AND`/`OR`/`NOT`
- **Sorting** — `orderBy()`, ascending/descending, multi-column sorts
- **Deduplication** — `distinct()`, `dropDuplicates()`, window-based dedup

### Aggregations
- **groupBy + agg** — `count`, `sum`, `avg`, `min`, `max`
- **Multi-column grouping** and **multiple aggregations** in a single pass
- **Filtering aggregated results** — `having`-style patterns
- **collect_list / concat_ws** — aggregating into lists and strings

### Joins
- **All join types** — inner, left, right, full outer, left_semi, left_anti, cross
- **Self joins** — joining a table with itself
- **Duplicate column handling** — aliasing and selecting after joins
- **Multi-column joins** — joining on composite keys
- **Source vs Target comparison** — Insert/Update/Delete detection patterns

### Window Functions
- `row_number()`, `rank()`, `dense_rank()`
- `lag()`, `lead()` — previous/next row access
- **Running totals** and **running averages** — `sum().over()`, `avg().over()`
- `first()`, `last()` — first/last value in a window
- **Top-N per group** — highest/second-highest salary per department
- **Window-based deduplication** — keep latest record per key

### Conditions, Strings, Dates & NULLs
- **Conditional logic** — `when()` / `otherwise()`, multi-branch conditions
- **String functions** — `upper`, `lower`, `trim`, `regexp_replace`, `substring`
- **Date functions** — `to_date`, `year`, `month`, `datediff`, `current_date`
- **NULL handling** — `isNull`, `isNotNull`, `fillna`, `dropna`, `coalesce`

### Working with Nested Data
- **ArrayType** — creating, sizing, filtering with `array_contains`, `explode`
- **Array extraction** — `element_at`, `slice` for positional access
- **Array transformations** — `array_distinct`, `array_sort`, `array_union`
- **MapType** — `map_keys`, `map_values`, `element_at` on maps, `map_contains_key`
- **StructType (nested)** — nested schema definition, dot-notation field access (`col("address.city")`)
- **JSON strings** — `from_json` to parse embedded JSON into StructType
- **Nested JSON with arrays** — `from_json` + `explode` pipeline
- **explode vs explode_outer** — handling empty arrays and NULLs

### File Format Operations
- **Parquet** — read, write, predicate pushdown, `explain()` physical plans
- **JSON** — flat & multiline reads, `explode` for arrays
- **CSV** — reading with headers, schema inference, handling malformed records

### Data Engineering Patterns
- **SCD Type 2** — detect changes, expire old records, insert new versions
- **Partitioning** — `partitionBy()` for write-time data organization
- **Bucketing** — `bucketBy()` + `sortBy()` + `saveAsTable()` for join optimization
- **Repartition vs Coalesce** — full shuffle vs partition merging
- **Predicate Pushdown** — Spark optimizer pushing filters to storage layer

### Spark SQL
- Running SQL queries on temporary views
- Equivalent SQL and DataFrame API patterns

---

## 📚 PySpark 100 Curriculum

The `pyspark_100/` directory contains a structured learning path with **75+ exercises completed** across 7 modules (plus revision scripts):

| Module | Topic | Exercises | Key APIs |
|--------|-------|-----------|----------|
| **Module 1** | Fundamentals | 10 | `createDataFrame`, `show`, `printSchema`, `select`, `withColumn`, `drop`, `cast` |
| **Module 2** | Filtering & Sorting | 10 | `filter`, `where`, `orderBy`, `distinct`, `dropDuplicates`, `limit` |
| **Module 3** | Aggregation | 10 | `groupBy`, `agg`, `count`, `sum`, `avg`, `min`, `max` |
| **Module 4** | Joins | 9 | `join` (inner, left, right, full, semi, anti), `alias`, self-join |
| **Module 5** | Conditions, Strings, Dates & NULLs | 11 | `when`, `otherwise`, `upper`, `trim`, `regexp_replace`, `to_date`, `datediff`, `fillna`, `coalesce` |
| **Module 6** | Window Functions | 15 | `Window.partitionBy`, `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first`, `last` |
| **Module 7** | Working with Nested Data | 10 | `ArrayType`, `MapType`, `StructType`, `explode`, `explode_outer`, `from_json`, `element_at`, `slice`, `array_distinct`, `array_sort`, `array_union`, `map_keys`, `map_values`, `map_contains_key` |
| **Revision** | Modules 1–3 | 4 | Comprehensive revision + final test |
| **Revision** | Modules 1–6 | 6 | End-to-end exercises: data cleaning, joins, aggregations, window functions, dedup |
| **Revision** | Module 7 | 5 | Arrays, maps, structs, JSON parsing, explode_outer |

> 📖 See [`pyspark_100/module_info.md`](pyspark_100/module_info.md) for the full curriculum roadmap covering all 12 planned modules.

---

## ⚙️ Local Setup

This project was developed and tested on the following local environment:

| Component | Version / Path |
|-----------|----------------|
| OS        | Windows        |
| Python    | `C:\python\python.exe` |
| Java (JDK) | 1.8 / 18.0.2 |
| Spark     | 3.5.1 (system-installed at `C:\spark\spark-3.5.1-bin-hadoop3`) |
| IDE       | PyCharm        |

### Prerequisites

- Java 8+ (JDK 1.8 or later)
- Python 3.x
- Apache Spark 3.5.1
- `pyspark` package (or system-installed Spark with `SPARK_HOME` configured)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/KaranKamat0506/pyspark_basics.git
   cd pyspark_basics
   ```

2. **Install PySpark** (if not using a system-level install)
   ```bash
   pip install pyspark
   ```

3. **Set environment variables** (if using a system-level Spark install)
   ```bash
   # Windows (PowerShell)
   $env:SPARK_HOME = "C:\spark\spark-3.5.1-bin-hadoop3"
   $env:JAVA_HOME  = "C:\Program Files\Java\jdk1.8.0_xxx"
   $env:PATH       = "$env:SPARK_HOME\bin;$env:PATH"
   ```

4. **Launch Jupyter Notebook** (for `pyspark.ipynb`)
   ```bash
   jupyter notebook
   ```

5. **Run individual scripts**
   ```bash
   python pyspark_questions/pysparkq1.py
   # or
   spark-submit pyspark_questions/pysparkq1.py
   ```

---

## 🚀 Getting Started

Open `pyspark.ipynb` for a guided walkthrough of PySpark fundamentals. For structured learning, follow the `pyspark_100/` modules in order. For targeted practice, explore:

- **`pyspark_questions/`** — Real-world scenario problems
- **`pyspark_interview/`** — Interview-style questions with detailed explanations
- **`joinsInPyspark/`** — Deep-dive into all join patterns

A minimal SparkSession to get started:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Basics") \
    .master("local[*]") \
    .getOrCreate()
```

---

## 🗂️ Highlights

### SCD Type 2 Implementation
Full Slowly Changing Dimension Type 2 pattern — detect changes, expire old records, insert new versions:

```python
# Detect changed records
df_changed = df_join.filter(
    (col("t.city") != col("s.city")) & col("s.city").isNotNull()
)

# Expire old records
df_expired = df_existing.join(df_changed.select("customer_id"), "customer_id", "left_semi") \
    .withColumn("end_date", current_date()) \
    .withColumn("current_flag", lit("N"))

# Final dimension = unchanged + expired + new inserts
df_final = df_unchanged.unionByName(df_expired).unionByName(df_insert)
```

### Source vs Target Comparison
Detect inserts, updates, and deletes using join patterns:

```python
# Records to INSERT (in source, not in target)
insert_df = source_df.join(target_df, "id", "left_anti")

# Records to DELETE (in target, not in source)
delete_df = target_df.join(source_df, "id", "left_anti")

# Records to UPDATE (in both, but values changed)
update_df = source_df.join(target_df, "id", "inner") \
    .filter(source_df.salary != target_df.salary)
```

### Repartition vs Coalesce
Deep comparison with partition distribution analysis using `spark_partition_id()`:

```python
# Repartition: full shuffle → even distribution
partitioned_df = df.repartition(4)

# Coalesce: no shuffle → uneven but faster
coalesced_df = df.coalesce(3)
```

### Monthly Sales Summary Report
End-to-end aggregation pipeline: CSV → date conversion → extract year/month → groupBy → sort:

```python
monthly_summary = df.groupBy("region", "product", "year", "month") \
    .agg(
        F.sum("sales_amount").alias("total_sales"),
        F.avg("sales_amount").alias("avg_sales"),
        F.count("*").alias("number_of_orders")
    )
```

---

## 📌 Roadmap

- [x] Core DataFrame operations (Module 1)
- [x] Filtering & sorting (Module 2)
- [x] Aggregations & groupBy (Module 3)
- [x] All join types + advanced patterns (Module 4 + joinsInPyspark)
- [x] Conditions, strings, dates & NULLs (Module 5)
- [x] Window functions deep-dive (Module 6 + windowFunctions)
- [x] Working with nested data — arrays, maps, structs, JSON (Module 7)
- [x] Parquet and JSON file I/O
- [x] Partitioning & bucketing
- [x] SCD Type 2 implementation
- [x] Repartition vs coalesce
- [x] Practice question set
- [x] Interview question set with explanations
- [ ] Module 8 — Deduplication & Data Quality
- [ ] Module 8 — ETL, CDC & SCD
- [ ] Module 9 — Reading & Writing Data (CSV, JSON, Parquet, Delta)
- [ ] Module 10 — Performance Optimization
- [ ] Module 11 — Spark Architecture & Core Concepts
- [ ] Module 12 — Integrated Interview Practice
- [ ] Spark Structured Streaming examples
- [ ] Kafka integration samples

---

## 📊 Repository Stats

| Metric | Count |
|--------|-------|
| Total Python scripts | **120** |
| Curriculum modules completed | **7 / 12** |
| Curriculum exercises completed | **75+** |
| Interview problems | **6** (with explanations) |
| Practice questions | **12+** |
| Data engineering patterns | **5** (SCD2, partitioning, bucketing, repartition, predicate pushdown) |

---

## 👤 Author

**Karan Kamat**
- GitHub: [@KaranKamat0506](https://github.com/KaranKamat0506)

---

*Built with curiosity and a lot of `df.show()` calls.* ⚡
