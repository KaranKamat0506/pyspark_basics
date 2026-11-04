#9_9_dropna_coalesce
# Practice Problem 49

# Using the same phone_df dataset:
#
# Requirements:
# 1. Remove rows where both primary_phone and alternate_phone are NULL.
# 2. Create a new column called final_phone using coalesce().
# 3. Give priority to primary_phone; if unavailable, use alternate_phone.
# 4. Display employee_id, name, primary_phone, alternate_phone, and final_phone.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

phone_data = [
    (1, "Rahul", None, "9876543210"),
    (2, "Priya", "9123456780", "9988776655"),
    (3, "Amit", None, None),
    (4, "Sneha", "8877665544", None)
]

phone_columns = [
    "employee_id",
    "name",
    "primary_phone",
    "alternate_phone"
]

phone_df = spark.createDataFrame(phone_data, phone_columns)

result_df = phone_df.dropna(
    how="all",
    subset=["primary_phone","alternate_phone"]
)
result_df = result_df.withColumn(
    "final_phone",
    F.coalesce(
        F.col("primary_phone"),
        F.col("alternate_phone")
    )
)
result_df.select("name","primary_phone","alternate_phone","final_phone").show()
