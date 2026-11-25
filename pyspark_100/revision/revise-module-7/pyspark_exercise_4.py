from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", '{"projects":["AWS Migration","Data Lake","ETL Pipeline"]}'),
    (2, "Priya", '{"projects":["Sales Dashboard","ETL Pipeline"]}'),
    (3, "Amit", '{"projects":["Spark Migration","Data Lake"]}'),
    (4, "Sneha", '{"projects":[]}'),
    (5, "Vikas", None)
]

employees_df = spark.createDataFrame(
    data,
    ["employee_id", "name", "project_details"]
)

#define the schema only for project details

project_schema = StructType([
    StructField(
        "projects",
        ArrayType(StringType())
    )
])

# This schema describes the structure inside the JSON string.
# Think of the JSON:
"""
{
    "projects": ["AWS Migration", "Data Lake", "ETL Pipeline"]
}
"""

"""
Its structure is:
- projects → ArrayType
- Each array element → StringType
"""

# Apply the schema using from json
result_df = employees_df.withColumn(
    "project_details",
    F.from_json("project_details",project_schema)
) #Basically we apply the schema here in the from_json function

result_df.show(truncate=False)
result_df.printSchema()

result_df = result_df.withColumn(
    "projects",
    F.col("project_details.projects")
)

result_df.show(truncate=False)

result_df = result_df.withColumn(
    "project",
    F.explode("projects")
)

result_df.show(truncate=False)

result_df.filter(F.col("project") == "ETL Pipeline").show(truncate=False)
