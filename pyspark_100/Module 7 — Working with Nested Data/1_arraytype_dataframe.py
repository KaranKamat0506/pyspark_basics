from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

data = [
    (1,"Rahul",["Python","SQL","PySpark"]),
    (2,"Priya",["SQL","Excel"]),
    (3,"Amit",["Python","Java","Spark"]),
    (4,"Sneha",["Python","SQL"])
]

schema = StructType([
    StructField("employee_id",IntegerType()),
    StructField("name",StringType()),
    StructField("skills",ArrayType(StringType()))
])

employees_df = spark.createDataFrame(data, schema)

employees_df.show(truncate = False)
employees_df.printSchema()

"""
Function	Purpose
F.size("skills")	Counts elements in an array
F.array_contains("skills", "Python")	Checks whether an array contains a value
F.explode("skills")	Converts array elements into separate rows
"""


result_df = employees_df.withColumn(
    "skill_count",
    F.size(F.col("skills"))
)

result_df.show(truncate=False)

result_df = employees_df.withColumn(
    "skills",
    F.explode("skills")
)
result_df.show()

"""
Practice Problem 66 — ArrayType basics
Use the same employees_df DataFrame.

Task 1: Display the employee name and the number of skills each employee has.

Task 2: Find employees who have Python in their skills array.

Task 3: Find employees who have SQL in their skills array.

Hints:
- F.size()
- F.array_contains()
- filter()
- select()
"""

result_df = employees_df.withColumn(
    "no_of_skills",
    F.size("skills")
)

result_df.show()

employees_df.filter(F.array_contains("skills","Python")).show()

employees_df.filter(F.array_contains("skills","SQL")).show()

