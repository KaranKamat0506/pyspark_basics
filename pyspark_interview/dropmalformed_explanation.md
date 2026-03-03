# Understanding dropMalformed in PySpark

## Sample CSV (employees.csv)

``` csv
id,name,age
1,Alice,25
2,Bob,thirty
3,Charlie,30
4,David
5,Eva,28
6,Frank,40,ExtraColumn
```

------------------------------------------------------------------------

## Using Explicit Schema with DROPMALFORMED

### PySpark Code

``` python
schema = "id INT, name STRING, age INT"

df = spark.read     .option("header", "true")     .option("mode", "DROPMALFORMED")     .schema(schema)     .csv("employees.csv")

df.show()
```

### Output

Only valid rows are retained:

    +---+-------+---+
    |id | name  |age|
    +---+-------+---+
    |1  |Alice  |25 |
    |3  |Charlie|30 |
    |5  |Eva    |28 |
    +---+-------+---+

Dropped rows: - Row 2 (invalid datatype: "thirty") - Row 4 (missing
column) - Row 6 (extra column)

------------------------------------------------------------------------

## Using inferSchema with DROPMALFORMED

### PySpark Code

``` python
df = spark.read     .option("header", "true")     .option("inferSchema", "true")     .option("mode", "DROPMALFORMED")     .csv("employees.csv")

df.show()
```

### What Happens?

Spark infers schema based on data. Because of value "thirty", the `age`
column is inferred as STRING instead of INT.

So schema becomes:

    id   -> INT
    name -> STRING
    age  -> STRING

### Output

    1,Alice,25
    2,Bob,thirty
    3,Charlie,30
    5,Eva,28

Row 2 is NOT dropped because "thirty" is valid STRING.

Rows dropped: - Row 4 (missing column) - Row 6 (extra column)

------------------------------------------------------------------------

## Key Interview Takeaway

-   `DROPMALFORMED` works strictly when schema is explicitly defined.
-   Using `inferSchema` may prevent datatype-based malformed rows from
    being dropped.
-   Best practice in production: Always define schema explicitly.
