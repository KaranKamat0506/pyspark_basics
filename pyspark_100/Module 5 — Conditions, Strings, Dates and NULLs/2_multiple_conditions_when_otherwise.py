#2_2_Multiple_conditions_when_otherwise
from pyspark.sql import SparkSession
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

employees_df = spark.createDataFrame(employee_data, columns)

result_df = employees_df.withColumn(
    "performance",
    F.when(F.col("salary") >= 80000, "Excellent")\
    .when(F.col("salary") >= 70000,"Good")\
    .when(F.col("salary") >= 60000,"Average")\
    .otherwise("Needs improvement")
)

result_df.select("name","salary","performance").show()

# Practice Problem 42

# Using the same employees_df dataset:

# Requirement: Create a new column called experience_category based on the following conditions.
#
# Assume the following experience rules are represented by salary for this exercise:
# 1. Salary greater than or equal to ₹80,000 → Senior
# 2. Salary greater than or equal to ₹70,000 → Mid-Level
# 3. Salary greater than or equal to ₹60,000 → Junior
# 4. Otherwise → Fresher
# Display only these three columns:
# - name
# - salary
# - experience_category

practice_df = employees_df.withColumn(
    "experience_category",
    F.when(F.col("salary") >= 80000,"Senior")\
    .when(F.col("salary") >= 70000,"Mid-Level")\
    .when(F.col("salary") >= 60000,"Junior")\
    .otherwise("Fresher")
)

practice_df.select("name","salary","experience_category").show()
