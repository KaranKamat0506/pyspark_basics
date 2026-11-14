# Final practice — Combined revision
# Test yourself · 4 minutes
# This exercise combines all three modules. Try solving it without referring to the examples above.
#
# Using the same df, complete these requirements:
# 1. Filter employees earning at least ₹60,000.
# 2. Add a column named bonus equal to 10% of salary.
# 3. Group the filtered employees by department.
# 4. Calculate employee count, total salary, and average salary.
# 5. Keep only departments whose total salary is greater than ₹200,000.
# 6. Sort the result by total salary in descending order.
# 7. Store the final DataFrame in result and display it using result.show().

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

# df = df.filter(F.col("salary") >= 60000)
# df = df.withColumn("bonus", (F.col("salary")*0.10))
# df = df.groupBy("department").agg(
#     F.count("*").alias("emp_count"),
#     F.sum("salary").alias("total_salary"),
#     F.avg("salary").alias("avg_salary")
# ).filter(F.col("total_salary") > 200000).orderBy(F.desc("total_salary"))

result = df.filter(F.col("salary") >= 60000)             .withColumn("bonus", (F.col("salary")*0.10))             .groupBy("department").agg(
                    F.count("*").alias("emp_count"),
                    F.sum("salary").alias("total_salary"),
                    F.avg("salary").alias("avg_salary")
                )             .filter(F.col("total_salary") > 200000).orderBy(F.desc("total_salary"))

result.show()
