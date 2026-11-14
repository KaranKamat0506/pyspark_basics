# Practice problem — Module 2
# Using df, write code to:
# 1. Find employees whose salary is at least ₹65,000.
# 2. Sort them by salary in ascending order.
# 3. Display only the first three rows of the sorted result.
# Hint: Combine filter(), orderBy(), and limit().

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

# df = spark.createDataFrame(data, columns)
df2 = spark.createDataFrame(data, columns)
#
# df = df.filter(df.salary >= 65000).orderBy(F.asc("salary")).limit(3)
# df.show()
#
# df = df.filter(F.col("department") != "Finance")
# df.show()

df2 = df2.filter((F.col("department") == "IT") | (F.col("department") == "Finance"))
df2.show()
