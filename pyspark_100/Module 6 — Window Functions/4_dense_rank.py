from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

ranking_data = [
    (1, "Rahul", "IT", 90000),
    (2, "Amit", "IT", 80000),
    (3, "Rohit", "IT", 80000),
    (4, "Priya", "IT", 70000),
    (5, "Neha", "HR", 85000),
    (6, "Sneha", "HR", 85000),
    (7, "Anjali", "HR", 75000)
]

ranking_columns = [
    "employee_id", "name", "department", "salary"
]

ranking_df = spark.createDataFrame(ranking_data, ranking_columns)

window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())

result_df = ranking_df.withColumn(
    "salary_dense_rank",
    F.dense_rank().over(window_spec)
)

result_df.select("name","department","salary","salary_dense_rank").orderBy("department","salary_dense_rank").show()
