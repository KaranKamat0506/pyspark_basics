#7_7_datediff
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

date_data = [
    (1, "Rahul", "2022-07-15"),
    (2, "Priya", "2023-03-20"),
    (3, "Amit", "2024-11-05"),
    (4, "Sneha", "2021-01-10")
]

date_columns = ["employee_id", "name", "joining_date"]

date_df = spark.createDataFrame(date_data, date_columns)

result_df = date_df.withColumn(
    "joining_date",
    F.to_date(F.col("joining_date"),"yyyy-MM-dd")
).withColumn(
    "current_date",
    F.current_date()
).withColumn(
    "days_worked",
    F.date_diff(F.col("current_date"),F.col("joining_date"))
)

result_df.select("name","joining_date","days_worked").show()

"""
Practice Problem 47
Using the same date_df dataset:
Requirements:
1. Convert joining_date into DateType.
2. Create a column called days_until_5_years that calculates how many days remain until the employee completes 5 years from their joining date.
3. Use date_add() to calculate the date exactly 5 years after joining. For this exercise, use 1825 days.
4. Use datediff() to calculate the difference between that date and today's date.
5. Display name, joining_date, five_year_completion_date, and days_until_5_years.
"""

result_df_2 = date_df.withColumn(
    "joining_date",
    F.to_date(F.col("joining_date"),"yyyy-MM-dd")
).withColumn(
  "five_year_completion_date",
    F.date_add(F.col("joining_date"),1825)
).withColumn(
    "days_until_5_years",
    F.date_diff(
        F.col("five_year_completion_date"),
        F.current_date()
    )
)

result_df_2.select("name","joining_date","five_year_completion_date","days_until_5_years").show()
