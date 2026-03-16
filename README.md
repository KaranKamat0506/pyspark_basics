# 🔥 PySpark Basics & Practice

> A hands-on collection of PySpark scripts, practice questions, and interview prep — built from the ground up while learning Apache Spark.

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange?logo=apachespark&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange?logo=jupyter&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

---

## 📖 About

This repository documents my journey learning **Apache Spark** through PySpark. It covers core concepts, file format operations, and curated practice questions — including a dedicated section for interview-style problems. All code is written in Python and tested in a local Spark environment.

---

## 📁 Repository Structure

```
pyspark_basics/
│
├── pyspark.ipynb                # Core PySpark notebook — fundamentals & experiments
│
├── pyspark_questions/           # Practice problems covering key PySpark concepts
│
├── pyspark_interview/           # Interview-style questions and solutions
│
├── parquetFileOperations/       # Reading, writing, and querying Parquet files
│
└── readFromJson/                # Loading and processing JSON data with PySpark
```

---

## 🧠 Topics Covered

- **SparkSession & SparkContext** — setting up and configuring a Spark application
- **DataFrames** — creating, transforming, filtering, and aggregating data
- **Spark SQL** — running SQL queries on temporary views
- **Schema Definition** — using `StructType` and `StructField` for explicit schemas
- **File Formats** — reading and writing Parquet and JSON files
- **RDDs** — basic RDD operations and transformations
- **Practice Problems** — real-world style questions for hands-on learning
- **Interview Prep** — commonly asked PySpark questions with solutions

---

## ⚙️ Local Setup

This project was developed and tested on the following local environment:

| Component | Version / Path |
|-----------|----------------|
| OS        | Windows        |
| Python    | `C:\python\python.exe` |
| Java (JDK) | 1.8            |
| Spark     | 3.5.1 (system-installed at `C:\spark\spark-3.5.1-bin-hadoop3`) |

### Prerequisites

- Java 8 (JDK 1.8)
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

4. **Launch Jupyter Notebook**
   ```bash
   jupyter notebook
   ```

---

## 🚀 Getting Started

Open `pyspark.ipynb` for a guided walkthrough of PySpark fundamentals. For targeted practice, explore the `pyspark_questions/` and `pyspark_interview/` folders.

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

### Parquet File Operations
Reading and writing columnar Parquet files — the standard format for big data pipelines.

```python
# Write
df.write.mode("overwrite").parquet("output/data.parquet")

# Read
df = spark.read.parquet("output/data.parquet")
df.show()
```

### JSON Ingestion
Loading nested and flat JSON structures into Spark DataFrames.

```python
df = spark.read.option("multiline", "true").json("data/sample.json")
df.printSchema()
df.show()
```

---

## 📌 Roadmap

- [x] Core DataFrame operations
- [x] Parquet and JSON file I/O
- [x] Practice question set
- [x] Interview question set
- [ ] Window functions deep-dive
- [ ] Spark Structured Streaming examples
- [ ] Kafka integration samples

---

## 👤 Author

**Karan Kamat**
- GitHub: [@KaranKamat0506](https://github.com/KaranKamat0506)

---

*Built with curiosity and a lot of `df.show()` calls.* ⚡
