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
Practice Problem 59
Using employees_df:
1. Partition employees by department.
2. Order by salary descending.
3. Use F.first_value() to retrieve the highest salary.
4. Use F.last_value() to retrieve the lowest salary.
5. Create columns highest_salary and lowest_salary.
6. Display name, department, salary, highest_salary, and lowest_salary.
"""

window_spec = (
    Window.partitionBy("department") \
    .orderBy(F.col("salary").desc()) \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

result_df = employees_df.withColumn(
    "lowest_salary",
    F.first_value("salary").over(window_spec)
).withColumn(
    "highest_salary",
    F.last_value("salary").over(window_spec)
)

result_df.select("name","department","salary","highest_salary","lowest_salary").orderBy("department",F.col("salary").desc()).show()