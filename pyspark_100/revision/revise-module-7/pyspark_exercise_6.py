from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", ("Mumbai", "Maharashtra", 400001)),
    (2, "Priya", ("Pune", "Maharashtra", 411001)),
    (3, "Amit", ("Bengaluru", "Karnataka", 560001)),
    (4, "Sneha", ("Delhi", "Delhi", 110001))
]

schema = StructType([
    StructField("employee_id", IntegerType()),
    StructField("name",StringType()),
    StructField("address",StructType([
        StructField("city",StringType()),
        StructField("state",StringType()),
        StructField("pincode", IntegerType())
    ]))
])

employee_df = spark.createDataFrame(data,schema)

result_df = employee_df.withColumn(
    "city",
    F.col("address.city")
).withColumn(
    "state",
    F.col("address.state")
).withColumn(
    "pincode",
    F.col("address.pincode")
)

result_df.show(truncate=False)
result_df.printSchema()
