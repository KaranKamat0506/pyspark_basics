"""
1. Prepare and analyze employee data
Covers Modules 1, 2, 3 and 5

Scenario: Your company has received employee data from an external source. Some records contain NULL values, and the salary column is stored as a string.

Your tasks:
1. Display the schema, total number of rows and columns.
2. Clean employee names by removing extra spaces and converting them to proper case.
3. Convert salary from StringType to IntegerType.
4. Convert joining_date to DateType.
5. Replace NULL salaries with the department's average salary, if you choose to handle it this way, or another clearly explained strategy.
6. Add a salary_category column:
   - Salary >= 80,000 → High
   - Salary >= 60,000 → Medium
   - Otherwise → Low
7. Find employees in IT or HR whose salary is greater than 60,000.
8. Sort employees by department and salary descending.
9. Calculate total salary, average salary and employee count for each department.
10. Find employees who have been with the company for more than 3 years.

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, current_date
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

employee_data = [
    (1, " Rahul ", "IT", "50000", "2022-01-10"),
    (2, "PRIYA", "HR", "65000", "2021-05-15"),
    (3, "Amit", "IT", "75000", "2023-03-20"),
    (4, "Sneha ", "Finance", "55000", "2022-08-11"),
    (5, "Neha", "HR", "80000", "2020-12-01"),
    (6, "Vikas", "Finance", None, "2021-07-19"),
    (7, "Rohit", "IT", "90000", "2019-04-25"),
    (8, "Anjali", "HR", "70000", None),
    (9, "Karan", "Finance", "85000", "2020-09-12"),
    (10, "Pooja", "IT", "65000", "2023-06-01")
]

columns = [
    "employee_id", "name", "department",
    "salary", "joining_date"
]

employees_df = spark.createDataFrame(employee_data, columns)

employees_df.printSchema()

print("Total rows: ", employees_df.count() )
print("Total columns ", len(employees_df.columns))

#2
result_df = employees_df.withColumn(
    "name",
    F.trim(F.col("name"))
).withColumn(
    "name",
    F.initcap(F.col("name"))
)

result_df.select("employee_id","name").show()

#3
result_df = employees_df.withColumn(
    "salary",
    F.col("salary").cast("integer")
)
result_df.printSchema()

#4
result_df = employees_df.withColumn(
    "joining_date",
    to_date("joining_date","yyyy-MM-dd")
)

result_df.printSchema()

#5
#use coalesce here
window = Window.partitionBy("department")

result_df = employees_df.withColumn(
    "avg_salary",
    F.avg("salary").over(window)
).withColumn(
    "salary",
    F.coalesce(F.col("salary"),F.col("avg_salary"))
)

result_df.show()

#6
result_df = result_df.withColumn(
    "salary_category",
    F.when(F.col("salary") >= 80000, "High" ) \
    .when(F.col("salary") >= 60000, "Medium") \
    .otherwise("Low")
)
result_df.show()

#7
result_df.filter(
    (
        (F.col("department") == "IT") |
        (F.col("department") == "HR")
     ) &
    (F.col("salary") > 60000)
).show()
#8
result_df.orderBy("department",F.col("salary").desc())

#9

agg_df = result_df.groupBy("department").agg(
    F.sum("salary").alias("Total Salary"),
    F.avg("salary").alias("Avg Salary"),
    F.count("*").alias("Employee_count")
)
agg_df.show()

#10
result_df = employees_df.withColumn(
    "days_of_service",
    F.date_diff(F.current_date(), F.col("joining_date"))
).withColumn(
    "years_of_service",
    F.floor(F.col("days_of_service")/365)
)

result_df.filter(F.col("years_of_service") > 3).show()
