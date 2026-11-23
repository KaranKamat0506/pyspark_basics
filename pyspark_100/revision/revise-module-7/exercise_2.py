from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", {"Python": 4, "SQL": 3, "PySpark": 2}),
    (2, "Priya", {"SQL": 3, "Excel": 2}),
    (3, "Amit", {"Python": 2, "Java": 3, "Spark": 1}),
    (4, "Sneha", {"Python": 3, "SQL": 2})
]

employees_df = spark.createDataFrame(
    data,
    ["employee_id", "name", "skill_experience"]
)

employees_df.printSchema()

"""Task 4 — Map operations
- Extract all keys into a column called skills.
- Extract all values into a column called experience.
- Extract Python experience into python_experience."""

task1df = employees_df.withColumn(
    "skills",
    F.map_keys("skill_experience")
).withColumn(
    "experience",
    F.map_values("skill_experience")
).withColumn(
    "python_experience",
    F.element_at("skill_experience","Python")
)
task1df.show(truncate=False)

"""
Task 5 — Map filtering
- Find employees who have Python as a key.
- Find employees whose Python experience is greater than 2 years.
"""

task1df.filter(F.map_contains_key(F.col("skill_experience"),"Python")).show(truncate=False)

task1df.filter(F.col("python_experience") > 2).show(truncate=False)
