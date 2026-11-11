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
Practice Problem 61
Using the same employees_df:

Find the top 3 highest-paid employees in each department.

Requirements:
1. Partition by department.
2. Order salary in descending order.
3. Generate a column named salary_rank using row_number().
4. Filter records where salary_rank is less than or equal to 3.
5. Display name, department, salary, and salary_rank.
6. Sort by department ascending and salary rank ascending.
"""

window_spec = Window.partitionBy("department") \
    .orderBy(F.col("salary").desc())

result_df = employees_df.withColumn(
    "salary_rank",
    F.row_number().over(window_spec)
).filter(F.col("salary_rank") <= 3)

result_df.select("name","department","salary","salary_rank").orderBy("department","salary_rank").show()