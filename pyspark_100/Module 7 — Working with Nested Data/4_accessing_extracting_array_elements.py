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

#use element_at for retrieving one element only
result_df = employees_df.withColumn(
    "first_skill",
    F.element_at("skills",1)
)

result_df.select("name","first_skill").show()

#slice function
result_df = employees_df.withColumn(
    "first_two_skills",
    F.slice("skills",1,2)
)

"""
Practice Problem 69
Using the existing employees_df DataFrame:
Task 1: Retrieve the first skill of every employee and create a column named first_skill.
Task 2: Retrieve the second skill of every employee and create a column named second_skill.
Task 3: Retrieve the first two skills of every employee and create a column named first_two_skills.
"""

print("Task 1")
result_df = employees_df.withColumn(
    "first_skill",
    F.element_at("skills",1)
).withColumn(
    "second_skill",
    F.element_at("skills",2)
).withColumn(
    "first_two_skills",
    F.slice("skills",1,2)
)

result_df.show(truncate=False)