#4_4_trim_regex_replace
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

dirty_data = [
    (1, "  Rahul  ", "987-654-3210"),
    (2, " Priya ", "912-345-6780"),
    (3, "Amit   ", "998-877-6655")
]

dirty_columns = ["employee_id", "name", "phone"]

dirty_df = spark.createDataFrame(dirty_data, dirty_columns)

cleaned_df = dirty_df.withColumn(
    "clean_name",
    F.trim(F.col("name"))
).withColumn(
    "clean_phone",
    F.regexp_replace(F.col("phone"),"-","")
)

cleaned_df.select("employee_id","clean_name","clean_phone")
cleaned_df.show()

"""
Practice Problem 44
Using the following dataset:
dirty_data_2 = [
    (1, "  Rahul Kamat  ", "Mumbai-India"),
    (2, " Priya Sharma ", "Pune-India"),
    (3, "Amit Patil   ", "Delhi-India"),
    (4, " Sneha Joshi ", "Bangalore-India")
]

Requirements:
1. Create a column clean_name by removing leading and trailing spaces using trim().
2. Create a column clean_location by replacing - with a space using regexp_replace().
3. Create a column upper_name by converting the cleaned name to uppercase.
4. Display employee_id, clean_name, clean_location, and upper_name.
"""

dirty_data_2 = [
    (1, "  Rahul Kamat  ", "Mumbai-India"),
    (2, " Priya Sharma ", "Pune-India"),
    (3, "Amit Patil   ", "Delhi-India"),
    (4, " Sneha Joshi ", "Bangalore-India")
]

dirty_columns_2=["employee_id","name","location"]

dirty_df_2 = spark.createDataFrame(dirty_data_2,dirty_columns_2)

cleaned_df_2 = dirty_df_2.withColumn(
    "clean_name",
    F.trim(F.col("name"))
).withColumn(
    "clean_location",
    F.regexp_replace(F.col("location"), "-", " ")
).withColumn(
    "upper_name",
    F.upper(F.col("clean_name"))
)

cleaned_df_2.select("employee_id","clean_name","clean_location","upper_name")
cleaned_df_2.show()
