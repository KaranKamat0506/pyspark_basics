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
Problem 58 — Practice
Using the existing employees_df dataset:

Requirements:
1. Partition employees by department.
2. Sort salary in descending order.
3. Calculate the running average salary.
4. Create a column named running_avg_salary.
5. Display name, department, salary, and running_avg_salary.
6. Sort the final output by department ascending and salary descending.
"""

window_spec = (
    Window.partitionBy("department") \
    .orderBy(F.col("salary").desc()) \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result_df = employees_df.withColumn(
    "running_avg_salary",
    F.avg("salary").over(window_spec)
)

result_df.select("name","department","salary","running_avg_salary").orderBy("department",F.col("salary").desc()).show()