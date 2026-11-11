from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

employee_data = [
    (1, "Rahul", "IT", 50000),
    (2, "Priya", "HR", 65000),
    (3, "Amit", "IT", 75000),
    (4, "Sneha", "Finance", 55000),
    (5, "Neha", "HR", 80000),
    (6, "Vikas", "Finance", 60000),
    (7, "Rohit", "IT", 90000),
    (8, "Anjali", "HR", 70000),
    (9, "Karan", "Finance", 85000),
    (10, "Pooja", "IT", 65000)
]

columns = ["employee_id", "name", "department", "salary"]

employees_df = spark.createDataFrame(employee_data,columns)

"""
Practice Problem 60
Using the same employees_df:
1. Partition employees by department.
2. Calculate the maximum salary using F.max().
3. Calculate the minimum salary using F.min().
4. Create max_salary and min_salary columns.
5. Create another column named salary_difference, calculated as maximum salary minus minimum salary.
6. Display name, department, salary, max_salary, min_salary, and salary_difference.
7. Sort by department ascending and salary descending.
"""

window_spec = (
    Window.partitionBy("department") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

result_df = employees_df.withColumn(
    "max_salary",
    F.max("salary").over(window_spec)
).withColumn(
    "min_salary",
    F.min("salary").over(window_spec)
).withColumn(
    "salary_difference",
    (F.col("max_salary") - F.col("min_salary"))
)

result_df.select("name","department","salary","max_salary","min_salary","salary_difference").orderBy("department",F.col("salary").desc()).show()