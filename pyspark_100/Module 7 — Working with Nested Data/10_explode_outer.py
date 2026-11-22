from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", ["AWS", "PySpark"]),
    (2, "Priya", []),
    (3, "Amit", ["Spark"]),
    (4, "Sneha", None)
]

employees_df = spark.createDataFrame(
    data,
    ["employee_id", "name", "projects"]
)

employees_df.withColumn(
    "project",
    F.explode("projects")
).show()

employees_df.withColumn(
    "project",
    F.explode_outer("projects")
).show()

# explode_outer -> Retains the employee row

