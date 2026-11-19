from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import  StructType, StructField,IntegerType,StringType,ArrayType

spark = SparkSession.builder.getOrCreate()

data = [
    (1,"Rahul",["Python","SQL","PySpark"]),
    (2,"Priya",["SQL","Excel"]),
    (3,"Amit",["Python","Java","Spark"]),
    (4,"Sneha",["Python","SQL"])
]

schema = StructType(
    [
        StructField("employee_id",IntegerType()),
        StructField("name",StringType()),
        StructField("skills",ArrayType(StringType()))
    ]
)

employees_df = spark.createDataFrame(data,schema)

result_df = employees_df.withColumn(
    "unique_skills",
    F.array_distinct("skills")
)
result_df.show(truncate=False)

result_df = employees_df.withColumn(
    "sorted_skills",
    F.array_sort("skills")
)
result_df.show(truncate=False)

result_df = employees_df.withColumn(
    "combined_skills",
    F.array_union(
        F.col("skills"),
        F.array(F.lit("AWS"), F.lit("Databricks"))
    )
)
result_df.show(truncate=False)

"""
Practice Problem 70
Using the existing employees_df DataFrame:
1. Remove duplicate skills from each employee's array.
2. Sort each employee's skills alphabetically.
3. Add "AWS" and "Databricks" to each employee's skills without introducing duplicates.
"""

result_df= employees_df.withColumn(
    "unique_skills",
    F.array_distinct("skills")
).withColumn(
    "sorted_salary",
    F.array_sort("skills")
).withColumn(
    "Combined_Skills",
    F.array_union(
        F.col("skills"),
        F.array(F.lit("AWS"),F.lit("Databricks"))
    )
)

result_df.show(truncate=False)