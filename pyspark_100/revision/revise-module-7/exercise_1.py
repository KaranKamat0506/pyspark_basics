from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", ["Python", "SQL", "PySpark", "Python"]),
    (2, "Priya", ["SQL", "Excel"]),
    (3, "Amit", ["Python", "Java", "Spark"]),
    (4, "Sneha", ["Python", "SQL"])
]

employees_df = spark.createDataFrame(
    data,
    ["employee_id", "name", "skills"]
)

"""
Task 1 — Array basics
- Find the number of skills each employee has.
- Filter employees who know Python.
- Filter employees who have more than 2 skills.
"""
result_df = employees_df.withColumn(
    "number_of_skills",
    F.size(F.col("skills"))
)

result_df.show(truncate=False)

result_df.filter(F.array_contains("skills","Python")).show(truncate=False)

result_df.filter(F.col("number_of_skills") > 2).show(truncate = False)

"""
Task 2 — Array transformations
- Remove duplicate skills from each employee's array.
- Sort the skills alphabetically.
- Add "AWS" and "Databricks" to every employee's skills without duplicates.
"""

task2_df = employees_df.withColumn(
    "skills",
    F.array_distinct(F.col("skills"))
).withColumn(
    "sorted_skills",
    F.array_sort(F.col("skills"))
).withColumn(
    "combined_skills",
    F.array_union(
        F.col("skills"),
        F.array(F.lit("AWS"),F.lit("Databricks"))
    )
)

task2_df.show(truncate=False)

"""
Task 3 — Array extraction
- Extract the first skill into first_skill.
- Extract the second skill into second_skill.
- Extract the first two skills into first_two_skills.
"""

task3_df = employees_df.withColumn(
    "first_skill",
    F.element_at(F.col("skills"),1)
).withColumn(
    "second_skill",
    F.element_at(F.col("skills"),2)
).withColumn(
    "first_two_skills",
    F.slice(F.col("skills"),1,2)
)

task3_df.show(truncate=False)
