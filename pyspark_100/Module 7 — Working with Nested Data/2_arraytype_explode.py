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
StructField("employee_id",StringType()),
    StructField("name",StringType()),
    StructField("skills",ArrayType(StringType()))
])

employees_df = spark.createDataFrame(data,schema)
employees_df.show()

"""
Using the existing employees_df:
- Apply F.explode() to the skills column.
- Create a new column called individual_skill.
- Display the employee name and individual skill.
"""

result_df = employees_df.withColumn(
    "individual_skill",
    F.explode("skills")
)

result_df.show()
result_df.select("name","individual_skill").show()