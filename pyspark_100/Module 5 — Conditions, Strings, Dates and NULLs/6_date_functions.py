#6_6_date_functions
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
    "joining_year",
    F.year(F.col("joining_date"))
).withColumn(
    "joining_month",
    F.month(F.col("joining_date"))
)

result_df.show()
result_df.printSchema()

"""
Practice Problem 46
Using the same date_df dataset:

Requirements:
1. Convert joining_date from StringType to DateType.
2. Create a column joining_year using year().
3. Create a column joining_month using month().
4. Create a column joining_day using day().
5. Create a column after_30_days by adding 30 days to joining_date using date_add().
6. Display name, joining_date, joining_year, joining_month, joining_day, and after_30_days.
"""

result_df_2=date_df.withColumn(
    "joining_date",
    F.to_date(F.col("joining_date"),"yyyy-MM-dd")
).withColumn(
    "joining_year",
    F.year(F.col("joining_date"))
).withColumn(
    "joining_month",
    F.month(F.col("joining_date"))
).withColumn(
    "joining_day",
    F.day(F.col("joining_date"))
).withColumn(
    "after_30_days",
    F.date_add(F.col("joining_date"),30)
)

result_df_2.select("name","joining_date","joining_year","joining_month","joining_day","after_30_days").show()
