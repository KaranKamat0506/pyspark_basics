from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", '{"projects":["AWS Migration","Data Lake","ETL Pipeline"]}'),
    (2, "Priya", '{"projects":["Sales Dashboard","ETL Pipeline"]}'),
    (3, "Amit", '{"projects":["Spark Migration","Data Lake"]}')
]

employees_df = spark.createDataFrame(
    data,
    ["employee_id", "name", "project_details"]
)

project_schema= StructType([
    StructField("projects",ArrayType(StringType()))
])

result_df = employees_df.withColumn(
    "project_details",
    F.from_json("project_details",project_schema)
)

result_df.printSchema()

result_df = result_df.withColumn(
    "projects",
    F.explode("project_details.projects")
)

result_df.select("employee_id","name","projects").show(truncate=False)

"""
Convert project_details from JSON string into a StructType.
Extract the projects array into a separate column called projects.
Explode projects into individual rows.
Filter only projects containing "ETL Pipeline".
"""

project_schema = StructType([
    StructField("projects",ArrayType(StringType()))
])

result_df = employees_df.withColumn(
    "project_details",
    F.from_json("project_details",project_schema)
).withColumn(
    "projects_array",
    F.col("project_details.projects")
).withColumn(
    "project",
    F.explode("project_details.projects")
)

result_df.show(truncate=False)

result_df.filter(F.col("project") == "ETL Pipeline").show(truncate=False)




