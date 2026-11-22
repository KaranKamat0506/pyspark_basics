from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (1,"Rahul",'{"city":"Mumbai","state":"Maharashtra"}'),
    (2,"Priya",'{"city":"Pune","state":"Maharashtra"}'),
    (3, "Amit", '{"city":"Bengaluru","state":"Karnataka"}'),
    (4, "Sneha",'{"city":"Delhi","state":"Delhi"}')
]

employees_df = spark.createDataFrame(
    data,
    ["employee_id","name","address"]
)

employees_df.printSchema()
employees_df.show()

# Convert JSON string into a struct

address_schema = StructType([
    StructField("city",StringType()),
    StructField("state",StringType())
])

result_df = employees_df.withColumn(
    "address",
    F.from_json("address",address_schema)
)

result_df.printSchema()
result_df.show()

"""
Convert the JSON string into a StructType column called address.
Extract the city into a separate column called city.
Extract the state into a separate column called state.
Filter employees whose city is "Pune".
"""

address_schema1 = StructType([
    StructField("city",StringType()),
    StructField("state",StringType())
])

result_df = employees_df.withColumn(
    "address",
    F.from_json("address",address_schema1)
)

result_df.printSchema()

result_df = result_df.withColumn(
    "city",
    F.col("address.city")
).withColumn(
    "state",
    F.col("address.state")
)

result_df.filter(F.col("city") == "Pune").show()
