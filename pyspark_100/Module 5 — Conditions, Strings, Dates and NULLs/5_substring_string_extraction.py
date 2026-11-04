#5_5_substring_string_extraction
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

employee_code_data = [
    (1, "EMP2024001"),
    (2, "EMP2024002"),
    (3, "EMP2024003"),
    (4, "EMP2024004")
]

employee_code_df = spark.createDataFrame(
    employee_code_data,
    ["employee_id", "employee_code"]
)

result_df = employee_code_df.withColumn(
    "joining_year",
    F.substring(F.col("employee_code"),4,4)
)

result_df.show()

"""
Practice Problem 45
Using the following dataset:
employee_data_2 = [
    (1, "EMP2025001", "9876543210"),
    (2, "EMP2024002", "9123456780"),
    (3, "EMP2023003", "9988776655"),
    (4, "EMP2022004", "8877665544")
]

employee_df_2 = spark.createDataFrame(
    employee_data_2,
    ["employee_id", "employee_code", "phone"]
)

Requirements:
1. Create a column joining_year by extracting the year from employee_code.
2. Create a column phone_last4 by extracting the last four digits from phone.
3. Display employee_id, employee_code, joining_year, and phone_last4.

"""

employee_data_2 = [
    (1, "EMP2025001", "9876543210"),
    (2, "EMP2024002", "9123456780"),
    (3, "EMP2023003", "9988776655"),
    (4, "EMP2022004", "8877665544")
]

employee_df_2 = spark.createDataFrame(
    employee_data_2,
    ["employee_id", "employee_code", "phone"]
)

result_df_2 = employee_df_2.withColumn(
    "joining_year",
    F.substring(F.col("employee_code"),4,4)
).withColumn(
    "phone_last4",
    F.substring(F.col("phone"),7,4)
)

result_df_2.select("employee_id", "employee_code", "joining_year", "phone_last4").show()
