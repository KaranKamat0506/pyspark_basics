#3_3_String_Functions
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
    "upper_name",F.upper(F.col("name"))
).withColumn(
    "lower_name",F.lower(F.col("name"))
).withColumn(
    "name_length",F.length(F.col("name"))
)

result_df.select("name","upper_name","lower_name","name_length")
result_df.show()

# Practice Problem 43
# Using the same employees_df dataset:
#
# Requirement: Create the following three columns:
# 1. formatted_name — Convert the employee name into proper title case using initcap().
# 2. name_length — Calculate the number of characters in the employee name using length().
# 3. name_upper — Convert the employee name into uppercase using upper().

practice_df = employees_df.withColumn(
    "formatted_name",F.initcap(F.col("name"))
).withColumn(
    "name_length",F.length(F.col("name"))
).withColumn(
    "name_upper",F.upper(F.col("name"))
)
practice_df.select("formatted_name","name_upper","name_length")
practice_df.show()
