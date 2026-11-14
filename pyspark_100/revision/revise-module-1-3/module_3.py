# Practice problem — Module 3
# Using df, write code to:
# 1. Group by department.
# 2. Calculate employee count, total salary, and maximum salary.
# 3. Keep departments with more than two employees.
# 4. Sort by maximum salary in descending order.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()

data = [
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

df = spark.createDataFrame(data,columns)

df = df.groupBy("department").agg(
    F.count("*").alias("employee_count"),
    F.sum("salary").alias("total_salary"),
    F.max("salary").alias("max_salary")
).filter(F.col("employee_count") > 2).orderBy(F.desc("max_salary"))

df.show()
