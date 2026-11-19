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

schema = StructType(
    [
        StructField("employee_id",IntegerType()),
        StructField("name",StringType()),
        StructField("skills",ArrayType(StringType()))
    ]
)

employees_df = spark.createDataFrame(data,schema)

"""
Task 1 — Find employees with more than 2 skills
- Calculate the number of skills for each employee.
- Filter employees whose skill count is greater than 2.
- Display employee_id, name, and skills.
"""

result_df = employees_df.withColumn(
    "skill_count",
    F.size("skills")
)
result_df.filter(F.col("skill_count") > 2).select("employee_id","name","skills").show(truncate=False)

"""
Task 2 — Find employees who have both Python and SQL
- Filter employees whose skills array contains both "Python" and "SQL".
- Display their names and skills.
"""

result_df.filter(
    (F.array_contains("skills","Python")) & (F.array_contains("skills","SQL"))
).select("name","skills").show(truncate=False)

"""
Task 3 —Find employees who do not have Java
- Filter employees whose skills array does not contain "Java".
"""

result_df.filter(
    ~ F.array_contains("skills","Java")
).show(truncate=False)