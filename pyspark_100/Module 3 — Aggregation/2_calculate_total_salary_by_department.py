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

result = df.groupBy("department").agg(
    F.sum("salary").alias("total_salary")
).orderBy(F.desc("total_salary")).show()

# 3. Approach
# 1. groupBy("department") groups employees according to their department.
# 2. F.sum("salary") adds the salaries of employees in each department.
# 3. alias("total_salary") renames the output column.
# 4. orderBy(F.desc("total_salary"))
# 5. show() displays the result.
