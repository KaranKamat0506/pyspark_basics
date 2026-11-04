from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "  rahul kamat  ", "IT", 80000, "2022-07-15"),
    (2, " PRIYA SHARMA ", None, 65000, "2023-03-20"),
    (3, "amit patil", "Finance", None, "2024-11-05"),
    (4, "  sneha joshi", None, 55000, "2021-01-10"),
    (5, "NEHA DESAI  ", "HR", 90000, "2020-06-25")
]

columns = [
    "employee_id",
    "name",
    "department",
    "salary",
    "joining_date"
]

df = spark.createDataFrame(data, columns)

result_df = df.withColumn(
    "clean_name",
    F.initcap(F.trim(F.col("name")))
).withColumn(
    "clean_department",
    F.when(F.col("department").isNull(),"Not Assigned").otherwise(F.col("department"))
).withColumn(
    "clean_salary",
    F.when(F.col("salary").isNull(),0).otherwise(F.col("salary"))
).withColumn(
    "joining_date",
    F.to_date(F.col("joining_date"),"yyyy-MM-dd")
).withColumn(
    "joining_year",
    F.year(F.col("joining_date"))
).withColumn(
    "salary_category",
    F.when(
        F.col("clean_salary") >= 80000,"High"
    ).when(
        (F.col("clean_salary") >= 60000) & (F.col("clean_salary") < 80000 ), "Medium"
    ).otherwise("Low")
).withColumn(
    "experience_days",
    F.datediff(F.current_date(), "joining_date")
)

result_df.select("employee_id",
"clean_name",
"clean_department",
"clean_salary",
"joining_date",
"joining_year",
"salary_category",
"experience_days").show()
