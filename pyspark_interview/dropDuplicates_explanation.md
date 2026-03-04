# Understanding dropDuplicates() in PySpark

## What is dropDuplicates()?

`dropDuplicates()` is a PySpark DataFrame function used to remove
duplicate rows.

It can:

-   Remove fully identical rows
-   Remove duplicates based on selected columns

------------------------------------------------------------------------

## 1. Removing Fully Duplicate Rows

### Example Input

  ---- ------- -----
  id   name    age

  1 1  Rohan   25 25
  2    Rohan   30
       Amit    
  ---- ------- -----

### Code

``` python
df.dropDuplicates().show()
```

### Output

  --- ------- ----
  1 2 Rohan   25
      Amit    30

  --- ------- ----

------------------------------------------------------------------------

## 2. Removing Duplicates Based on Specific Columns

### Example Input

  ---- ------- -----
  id   name    age

  1 2  Rohan   25 28
  3    Rohan   30
       Amit    
  ---- ------- -----

### Code

``` python
df.dropDuplicates(["name"]).show()
```

### Output

  --- ------- ----
  1 3 Rohan   25
      Amit    30

  --- ------- ----

Note: Spark keeps the first occurrence found (order not guaranteed).

------------------------------------------------------------------------

## How It Works Internally

1.  Spark converts dropDuplicates into a GROUP BY operation.
2.  A shuffle is performed to bring identical keys into the same
    partition.
3.  Hash aggregation keeps the first row per key and discards
    duplicates.

Execution plan typically shows:

HashAggregate Exchange (Shuffle) HashAggregate

------------------------------------------------------------------------

## distinct() vs dropDuplicates()

-   `distinct()` removes fully identical rows.
-   `dropDuplicates()` can remove duplicates on selected columns.

`distinct()` is equivalent to:

``` python
df.dropDuplicates()
```

------------------------------------------------------------------------

## Performance Considerations

-   Causes shuffle (expensive on large datasets)
-   Uses memory to maintain hash map
-   High-cardinality columns increase memory usage
-   May spill to disk if memory is insufficient

------------------------------------------------------------------------

## Interview Summary

dropDuplicates() removes duplicate rows either completely or based on
selected columns.\
Internally, Spark performs shuffle and hash aggregation to detect
duplicates.
