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
Practice Problem 63
Using the same employees_df:

Find the second-highest distinct salary in each department, including the employee names.

Requirements:
1. Partition by department.
2. Sort salary in descending order.
3. Use F.dense_rank().
4. Create salary_rank.
5. Filter where salary_rank == 2.
6. Display name, department, salary, and salary_rank.
7. Sort by department.
"""

window_spec = Window.partitionBy("department") \
    .orderBy(F.col("salary"))

result_df = employees_df.withColumn(
    "salary_rank",
    F.dense_rank().over(window_spec)
).filter(F.col("salary_rank") == 2)

result_df.select("name","department","salary","salary_rank").orderBy("department","salary_rank").show()