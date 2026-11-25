from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
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

result_df_wo_outer = employees_df.withColumn(
    "project",
    F.explode("projects")
)

result_df_wo_outer.show(truncate=False)

result_df_outer = employees_df.withColumn(
    "project",
    F.explode_outer("projects")
)

result_df_outer.show(truncate = False)
