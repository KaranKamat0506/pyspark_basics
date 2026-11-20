from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data =[
    (1, "Rahul",{"Python":4, "SQL":3,"PySpark":2}),
    (2,"Priya",{"SQL":3, "Excel":2}),
    (3,"Amit",{"Python":2, "Java":3, "Spark":1}),
    (4,"Sneha",{"Python":3,"SQL":2})
]

schema = StructType([
    StructField("employee_id", IntegerType()),
    StructField("name",StringType()),
    StructField("skills_experience", MapType(StringType(), IntegerType()))
])

employees_df = spark.createDataFrame(data,schema)

"""
Your practice tasks
Task 1: Extract all skill names into a new column called skills.
Task 2: Extract all experience values into a new column called experience.
Task 3: Extract the Python experience into a new column called python_experience.
Task 4: Filter employees who have Python as a key in their map.
"""

result_df = employees_df.withColumn(
    "skills",
    F.map_keys("skills_experience")
).withColumn(
    "experience",
    F.map_values("skills_experience")
).withColumn(
    "python_experience",
    F.element_at("skills_experience",F.lit("Python"))
)

result_df.show(truncate=False)

result_df.filter(
    F.map_contains_key("skills_experience","Python")
).show(truncate=False)