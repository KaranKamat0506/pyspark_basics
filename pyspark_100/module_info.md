# PySpark 100 Problems --- Complete Learning Roadmap

A practical, hands-on learning roadmap for developing PySpark skills
used in Data Engineering workflows and technical interviews.

The curriculum moves from DataFrame fundamentals to advanced
transformations, ETL design, file processing, performance optimization,
Spark concepts, and integrated interview-style exercises.

## Learning Environment

-   **Language:** Python
-   **Framework:** Apache Spark / PySpark
-   **Development environment:** PyCharm (local setup)
-   **PySpark version:** 3.5.1
-   **Python executable:** `C:\python\python.exe`
-   **Spark distribution:** `C:\spark\spark-3.5.1-bin-hadoop3`
-   **Java:** 18.0.2

All coding exercises are designed for local PySpark. Examples use
`SparkSession`, DataFrame APIs, and `.show()` to inspect results.

------------------------------------------------------------------------

# Core PySpark Curriculum

## Module 1 --- PySpark Fundamentals

**Problems 1--10**

Learn how to create, inspect, and modify PySpark DataFrames.

Topics:

-   Creating DataFrames from Python collections
-   Defining column names and schemas
-   Displaying records with `show()`
-   Inspecting schemas with `printSchema()`
-   Counting rows and columns
-   Selecting columns
-   Renaming columns with `withColumnRenamed()`
-   Adding columns with `withColumn()` and `lit()`
-   Dropping columns with `drop()`
-   Casting columns to appropriate data types
-   Converting string values to dates

## Module 2 --- Filtering and Sorting

**Problems 11--20**

Learn how to retrieve records that satisfy specific conditions and
arrange the results.

Topics:

-   Filtering with `filter()` and `where()`
-   Comparison operators
-   Combining conditions with `&` and `|`
-   Negating conditions with `~`
-   Applying multiple filter conditions
-   Sorting with `orderBy()` and `sort()`
-   Ascending and descending order
-   Sorting by multiple columns
-   Limiting output with `limit()`

## Module 3 --- Aggregations and GROUP BY

**Problems 21--31**

Learn how to summarize records and calculate metrics at different
grouping levels.

Topics:

-   Grouping records with `groupBy()`
-   `count()`
-   `sum()`
-   `avg()`
-   `max()` and `min()`
-   Multiple aggregations with `agg()`
-   Grouping by multiple columns
-   Filtering aggregated results (HAVING-style logic)
-   Sorting aggregated results
-   Combining grouping, aggregation, and filtering

## Module 4 --- Joins

**Problems 32--40**

Learn how to combine DataFrames using common keys and understand the
behavior of different join types.

Topics:

-   Inner Join
-   Left Join
-   Right Join
-   Full Outer Join
-   Left Anti Join
-   Left Semi Join
-   Joining on multiple columns
-   Handling duplicate column names after joins
-   Self Join
-   Understanding which records are retained by each join type

## Module 5 --- Conditions, Strings, Dates and NULL Handling

**Problems 41--50**

Learn how to build conditional transformations and clean common
data-quality issues.

Topics:

-   Conditional columns with `when()` and `otherwise()`
-   Multiple conditional branches
-   String functions
-   Trimming whitespace with `trim()`
-   Replacing patterns with `regexp_replace()`
-   String extraction with `substring()`
-   Date conversion and date functions
-   Date differences with `datediff()`
-   Handling NULL values
-   Removing NULL records with `dropna()`
-   Replacing or filling NULL values
-   Using `coalesce()` for fallback values
-   Conditional NULL handling

## Module 6 --- Window Functions

**Problems 51--65**

Learn how to perform calculations across related rows while retaining
the individual records in a DataFrame.

Topics:

-   Window specifications
-   `Window.partitionBy()`
-   `Window.orderBy()`
-   Applying functions with `.over()`
-   Difference between `groupBy()` and window functions
-   `row_number()`
-   `rank()` and `dense_rank()`
-   `lag()` and `lead()`
-   Running totals
-   Running averages
-   `first_value()` and `last_value()`
-   Window-based maximum and minimum
-   Top N records per group
-   Highest salary per department
-   Second-highest distinct value per group
-   Deduplication using window functions
-   Combining multiple window functions

## Module 7 --- Deduplication and Data Quality

**Problems 66--75**

Learn how to identify duplicate records, apply business rules to retain
valid records, and validate incoming data.

Topics:

-   Identifying duplicate records
-   Removing duplicates with `dropDuplicates()`
-   Understanding the limitations of `dropDuplicates()`
-   Retaining the latest record using window functions
-   Retaining the earliest record using window functions
-   Handling duplicate business keys
-   Checking mandatory columns for NULL values
-   Validating data types and column values
-   Applying data-quality rules
-   Finding invalid records
-   Separating valid and rejected records
-   Building reusable data-quality checks

## Module 8 --- ETL, Incremental Processing, CDC and SCD

**Problems 76--85**

Learn how to implement common Data Engineering transformation patterns.

Topics:

-   Understanding source-to-target ETL flow
-   Comparing source and target DataFrames
-   Identifying new records
-   Identifying changed records
-   Identifying unchanged records
-   Implementing insert logic
-   Implementing update logic
-   Incremental data processing
-   Change Data Capture (CDC) concepts and transformations
-   Handling insert, update, and delete events
-   Slowly Changing Dimensions (SCD)
-   Implementing SCD Type 1
-   Implementing SCD Type 2
-   Maintaining effective dates and current-record indicators
-   Designing idempotent processing logic

## Module 9 --- Reading and Writing Data

**Problems 86--90**

Learn how to read data from common file formats and write transformed
DataFrames to storage.

Topics:

-   Reading CSV files
-   Reading JSON files
-   Reading Parquet files
-   Defining and applying schemas
-   Inferring schemas
-   Handling headers and delimiters
-   Write modes: append, overwrite, ignore, and error
-   Writing DataFrames to CSV, JSON, and Parquet
-   Partitioned output
-   Understanding the effect of partition columns on output files

## Module 10 --- PySpark Performance Optimization

**Problems 91--100**

Learn how Spark executes transformations and how to improve the
efficiency of PySpark jobs.

Topics:

-   Narrow and wide transformations
-   Lazy evaluation
-   Understanding shuffle operations
-   Using `repartition()`
-   Using `coalesce()`
-   Broadcast joins
-   Caching and persistence
-   Understanding data skew
-   Reducing unnecessary shuffles
-   Choosing appropriate partition counts
-   Optimizing joins and aggregations
-   Reading and interpreting execution plans with `explain()`
-   Practical Spark job optimization considerations

------------------------------------------------------------------------

# Additional Learning Modules

These modules complement the 100-problem coding curriculum with Spark
internals and interview preparation.

## Module 11 --- Spark Architecture and Core Concepts

Understand how Apache Spark works internally and how a PySpark
application is executed.

Topics:

-   Apache Spark overview and components
-   Driver and Executor
-   Cluster Manager
-   SparkSession and SparkContext
-   Jobs, stages, and tasks
-   DAG (Directed Acyclic Graph)
-   Transformations and actions
-   Lazy evaluation
-   Logical and physical execution plans
-   Catalyst Optimizer
-   Tungsten execution engine
-   Shuffles and stage boundaries
-   Narrow and wide dependencies
-   Memory management fundamentals
-   Fault tolerance and lineage
-   Spark UI fundamentals

## Module 12 --- Integrated PySpark Interview Practice

Apply concepts from the earlier modules to practical Data Engineering
scenarios and interview-style questions.

Topics:

-   DataFrame transformation challenges
-   Complex filtering and aggregation problems
-   Multi-DataFrame join scenarios
-   Window-function interview problems
-   Deduplication and latest-record selection
-   Data-quality and NULL-handling scenarios
-   Incremental load and CDC scenarios
-   SCD Type 1 and Type 2 scenarios
-   File ingestion and output-format questions
-   Performance troubleshooting scenarios
-   Debugging incorrect PySpark results
-   Explaining solution choices and trade-offs

------------------------------------------------------------------------

# Common PySpark Setup

The following setup is used in the local coding exercises:

``` python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
```

For DataFrame output, use:

``` python
df.show()
df.printSchema()
```

## Exercise Format

Each coding problem follows a practical learning approach:

1.  **Concept:** Understand the PySpark feature and when to use it.
2.  **Example:** Review a complete runnable example with its dataset.
3.  **Expected output:** Compare the result with the expected DataFrame
    output.
4.  **Practice:** Solve a separate exercise using the same or a new
    dataset.
5.  **Code review:** Check correctness, understand errors, and discuss
    alternative approaches.

The focus is on understanding the behavior of PySpark transformations
and applying them to realistic Data Engineering use cases.

------------------------------------------------------------------------

## Technologies and APIs Covered

-   Python
-   Apache Spark
-   PySpark DataFrame API
-   Spark SQL functions
-   Window functions
-   CSV, JSON, and Parquet
-   ETL and incremental data processing
-   CDC and SCD concepts
-   Data quality and validation
-   Spark execution and performance optimization

## Repository Structure

A suggested folder structure for organizing the exercises:

``` text
pyspark-100/
│
├── Module 1 - Fundamentals/
├── Module 2 - Filtering and Sorting/
├── Module 3 - Aggregations and Group By/
├── Module 4 - Joins/
├── Module 5 - Conditions Strings Dates and NULLs/
├── Module 6 - Window Functions/
├── Module 7 - Deduplication and Data Quality/
├── Module 8 - ETL CDC and SCD/
├── Module 9 - Reading and Writing Data/
├── Module 10 - Performance Optimization/
├── Module 11 - Spark Architecture and Core Concepts/
└── Module 12 - Integrated Interview Practice/
```

------------------------------------------------------------------------

*This repository is intended as a practical PySpark learning and
interview-preparation resource.*
