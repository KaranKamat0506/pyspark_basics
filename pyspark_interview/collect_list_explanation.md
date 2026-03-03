# Understanding collect_list() in PySpark

## What is collect_list()?

`collect_list()` is an aggregation function in PySpark that:

> Collects all values of a column into an array (list) after grouping.

It is typically used with `groupBy()`.

------------------------------------------------------------------------

## Sample Input Data

``` text
+-------+------+

|roll_no|Name  |

+-------+------+

|30     |Rohan |

|31     |Rohan |

|32     |Rohan |

+-------+------+
```

------------------------------------------------------------------------

## Basic Example

### PySpark Code

``` python
from pyspark.sql import functions as F

data = [(30, "Rohan"),
        (31, "Rohan"),
        (32, "Rohan")]

df = spark.createDataFrame(data, ["roll_no", "Name"])

result = df.groupBy("Name")            .agg(F.collect_list("roll_no").alias("roll_numbers"))

result.show(truncate=False)
```

------------------------------------------------------------------------

## Output

``` text
+------+-------------+

|Name  |roll_numbers |

+------+-------------+

|Rohan |[30, 31, 32] |

+------+-------------+
```

The roll numbers are collected into an array.

------------------------------------------------------------------------

## Important Interview Points

### 1. It Keeps Duplicates

If input contains:

``` text
30
30
31
```

Output will be:

``` text
[30, 30, 31]
```

------------------------------------------------------------------------

### 2. Order is NOT Guaranteed

Spark is distributed, so order may not be preserved.

To sort the array:

``` python
F.sort_array(F.collect_list("roll_no"))
```

------------------------------------------------------------------------

### 3. Difference Between collect_list() and collect_set()

  Function       Keeps Duplicates   Removes Duplicates
  -------------- ------------------ --------------------
  collect_list   Yes                No
  collect_set    No                 Yes

------------------------------------------------------------------------

## When NOT to Use collect_list()

-   Avoid using it on very large datasets without proper grouping.
-   It can cause memory issues because it collects data into arrays per
    group.
-   Be careful in production pipelines handling big data.

------------------------------------------------------------------------

## Interview Takeaway

-   `collect_list()` groups values into an array.
-   Keeps duplicates.
-   Does not guarantee order.
-   Use `sort_array()` if ordering is required.
