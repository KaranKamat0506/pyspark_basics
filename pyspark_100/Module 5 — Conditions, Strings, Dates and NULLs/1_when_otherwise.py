#1_1_when_otherwise
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
    "salary_status",
    F.when(F.col("salary") >= 70000,"High")
    .otherwise("Low")
)
result_df.show()

practice_df = employees_df.withColumn(
    "bonus",
    F.when(F.col("salary") >= 80000,10000)
    .when(
        (F.col("salary") >= 60000) & (F.col("salary") < 80000), 5000)
    .otherwise(2000)
)
practice_df.show()
