from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

employee_data = [
    (101, "  rohan ", "IT", "55000", "2021-04-10"),
    (102, "PRIYA", "HR", "72000", "2020-06-15"),
    (103, " amit ", "IT", "85000", "2022-01-20"),
    (104, "Sneha", "Finance", "60000", "2023-08-11"),
    (105, "NEHA", "HR", "90000", "2019-12-01"),
    (106, "Vikas ", "Finance", None, "2021-07-19"),
    (107, "ROHIT", "IT", "95000", "2018-04-25"),
    (108, " anjali", "HR", "68000", None),
    (109, "Karan", "Finance", "88000", "2020-09-12"),
    (110, "POOJA", "IT", "65000", "2023-06-01")
]

columns = [
    "employee_id",
    "name",
    "department",
    "salary",
    "joining_date"
]

employees_df = spark.createDataFrame(employee_data, columns)

"""
Task 1 — DataFrame inspection
- Display all employee records.
- Display the schema.
- Find the total number of rows.
- Find the total number of columns.
"""

employees_df.show()

employees_df.printSchema()

print(employees_df.count())

print(len(employees_df.columns))
#columns to show which are the columns
#len functions to calculate the number of the columns

"""
Task 2 — Name cleaning
- Remove leading and trailing spaces.
- Convert employee names into proper case.
- Display only employee ID and employee name.
"""

result_df = employees_df.withColumn(
    "name",
    F.trim("name")
).withColumn(
    "name",
    F.initcap("name")
)

result_df.select("employee_id","name").show()

"""
Task 3 — Data type conversion
- Convert salary from StringType to IntegerType.
- Convert joining_date from StringType to DateType.
- Display the updated schema.
"""

result_df = employees_df.withColumn(
    "salary",
    F.col("salary").cast("integer")
).withColumn(
    "joining_date",
    F.to_date(F.col("joining_date"),"yyyy-MM-dd")
)

result_df.printSchema()

"""
Task 4 — NULL handling
- Find records where salary is NULL.
- Replace NULL salary values with 0.
- Find records where joining_date is NULL.
"""

employees_df.filter(F.col("salary").isNull()).show()

result_df = employees_df.fillna(
    {"salary" : 0}
)
result_df.show()

employees_df.filter(F.col("joining_date").isNull()).show()

"""
Task 5 — Create a Salary Category
- Create a new column named salary_category.
- If salary >= 85,000 → "High"
- If salary >= 65,000 → "Medium"
- Otherwise → "Low"
"""

result_df = employees_df.withColumn(
    "salary_category",
    F.when(
        F.col("salary") >= 85000,
        "High"
    ).when(
        F.col("salary") >= 65000,
        "Medium"
    ).otherwise(
        "Low"
    )
)

result_df.show()

"""
Task 6 — Filter Records
- Find employees belonging to IT or Finance.
- From those employees, display only records where salary is greater than 70,000.
"""

employees_df.filter(
    ((F.col("department") == "IT") | (F.col("department") == "Finance") ) & (F.col("salary") >= 70000)
).show()

"""
Task 7 — Sorting
- Sort employees by department in ascending order.
- Within each department, sort salary in descending order.
"""

order_df = employees_df.orderBy("department", F.col("salary").desc())
order_df.show()

"""
Task 8 — GroupBy and Aggregations.

Group employees by department and calculate:
- Total salary
- Average salary
- Maximum salary
- Minimum salary
- Employee count
"""

result_df = employees_df.groupBy("department").agg(
    F.sum("salary").alias("Total Salary"),
    F.avg("salary").alias("Avg Salary"),
    F.min("salary").alias("Min Salary"),
    F.max("salary").alias("Max Salary"),
    F.count("*").alias("Employee Count")
)
result_df.show()

"""
Task 9 — Filter Aggregated Results

- Find departments where average salary is greater than 70,000.
"""
result_df.filter(F.col("Avg Salary") > 70000).show()

"""
Task 10 — Date Calculations
- Calculate each employee's days of service using joining_date.
- Calculate approximate years of service.
- Display only employees with more than 4 years of service.
- Show name, joining_date, days_of_service, and years_of_service.
"""

date_df = employees_df.withColumn(
    "days_of_service",
    F.datediff(F.current_date(), F.col("joining_date"))
).withColumn(
    "years_of_service",
    F.floor(
        F.datediff(
            F.current_date(),
            F.col("joining_date")
        ) / 365
    )
)

date_df.select("name", "joining_date", "days_of_service", "years_of_service").filter(F.col("years_of_service") > 4).show()



