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

df = spark.createDataFrame(data, columns)

# 3. Approach
# 1. groupBy("department") groups employees by department.
# 2. F.sum("salary") calculates the total salary per department.
# 3. alias("total_salary") gives the aggregated column a name.
# 4. filter(F.col("total_salary") > 210000) keeps only departments whose total salary exceeds ₹210,000.

result = df.groupBy("department").agg(
    F.avg("salary").alias("avg_salary")
).filter(
    F.col("avg_salary") > 70000
)

result.show()
