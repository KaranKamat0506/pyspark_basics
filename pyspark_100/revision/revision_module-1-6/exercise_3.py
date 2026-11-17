"""
Your tasks:
1. Calculate the average salary for each department using groupBy().
2. Add a department-wise row_number().
3. Add a department-wise rank().
4. Add a department-wise dense_rank().
5. Find the top 2 employees in each department.
6. Find employees earning the second-highest distinct salary in each department.
7. Calculate the running total of salaries within each department, ordered by salary descending.
8. Calculate the previous employee's salary using lag().
9. Calculate the next employee's salary using lead().
10. Add department-wise maximum and minimum salaries to every employee row.

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

salary_data = [
    (101, "Rahul", "IT", 90000),
    (102, "Amit", "IT", 80000),
    (103, "Rohit", "IT", 80000),
    (104, "Pooja", "IT", 65000),
    (105, "Priya", "HR", 85000),
    (106, "Neha", "HR", 85000),
    (107, "Anjali", "HR", 75000),
    (108, "Sneha", "Finance", 70000),
    (109, "Karan", "Finance", 90000),
    (110, "Vikas", "Finance", 60000)
]

salary_columns = [
    "employee_id", "name", "department", "salary"
]

salary_df = spark.createDataFrame(
    salary_data,
    salary_columns
)

print("Salary Df")
salary_df.show()

window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())

"""
1] Calculate the average salary for each department using groupBy().
"""
result_df = salary_df.groupBy("department").agg(
    F.avg("salary").alias("average_salary")
)

"""
2] Add a department-wise row_number().
3] Add a department-wise rank().
4] Add a department-wise dense_rank().
"""

result_df = salary_df.withColumn(
    "row_number",
    F.row_number().over(window_spec)
).withColumn(
    "rank",
    F.rank().over(window_spec)
).withColumn(
    "dense_rank",
    F.dense_rank().over(window_spec)
)

print("Result Df")
result_df.show()

print("Find the top 2 employees in each department.")
result_df.filter(
    (F.col("row_number") <= 2)
).show()

print("Find employees earning the second-highest distinct salary in each department.")
result_df.filter(
    (F.col("dense_rank") == 2 )
).show()

print("Calculate the running total of salaries within each department, ordered by salary descending.")
window_running_total = Window.partitionBy("department") \
    .orderBy(F.col("salary")) \
    .rowsBetween(Window.unboundedPreceding,Window.currentRow)

result_df = salary_df.withColumn(
    "running_total_salary",
    F.sum("salary").over(window_running_total)
)
result_df.show()

print("Previous Employees salary using lag()")

result_df = salary_df.withColumn(
    "previous_employee_salary",
    F.lag("salary").over(window_spec)
)
result_df.show()

print("Calculate the next employee's salary using lead()")

result_df = salary_df.withColumn(
    "next_employee_salary",
    F.lead("salary").over(window_spec)
)
result_df.show()

print("Add department-wise maximum and minimum salaries to every employee row.")

window_min_max_spec = Window.partitionBy("department") \
    .orderBy(F.col("salary").desc()) \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result_df = salary_df.withColumn(
    "Minimum_Salary",
    F.min("salary").over(window_min_max_spec)
).withColumn(
    "Maximum_Salary",
    F.max("salary").over(window_min_max_spec)
)
result_df.show()
