from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

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

employees_df.show()

"""
Your practice — Problem 52
Use the same employees_df DataFrame.

Task: Assign row numbers to employees within each department, but this time sort employees by salary in ascending order.

Requirements:
1. Partition by department.
2. Sort salary from lowest to highest.
3. Create a new column named salary_rank.
4. Display name, department, salary, and salary_rank.
5. Sort the final output by department and salary rank.
"""

window_spec = Window.partitionBy("department").orderBy("salary")

result_df = employees_df.withColumn(
    "salary_rank",
    F.row_number().over(window_spec)
)

result_df.select("name","department","salary","salary_rank").show()