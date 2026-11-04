#8_8_null_handling
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

null_data = [
    (1, "Rahul", "IT", 50000),
    (2, "Priya", None, 65000),
    (3, "Amit", "IT", None),
    (4, "Sneha", "Finance", 55000),
    (5, "Neha", None, 80000)
]

null_columns = ["employee_id", "name", "department", "salary"]

null_df = spark.createDataFrame(null_data, null_columns)

null_df.filter(F.col("department").isNull()).show()
null_df.filter(F.col("salary").isNotNull()).show()
result_df = null_df.fillna(
    {"department" : "Unknown"}
)
result_df.show()

# Practice Problem 48
# Using the same null_df dataset:
#
# Requirements:
# 1. Find all employees whose salary is NULL.
# 2. Find all employees whose department is NOT NULL.
# 3. Replace NULL salary values with 0.
# 4. Replace NULL department values with "Not Assigned".
# 5. Display the updated DataFrame.

print("Employees whose salary is Null")
null_df.filter(F.col("salary").isNull()).show()

print("Employees whise department is Not Null")
null_df.filter(F.col("department").isNotNull()).show()

print("Replace Null salary values with 0")
result_df_2 = null_df.fillna(
    {"salary":0}
)
result_df_2.show()

print("Replace NULL department values with Not Assigned")

result_df_2=null_df.fillna(
    {"department":"Not Assigned"}
)

result_df_2.show()
