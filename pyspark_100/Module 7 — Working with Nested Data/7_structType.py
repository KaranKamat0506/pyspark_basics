from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", ("Mumbai", "Maharashtra", 400001)),
    (2, "Priya", ("Pune", "Maharashtra", 411001)),
    (3, "Amit", ("Bengaluru", "Karnataka", 560001)),
    (4, "Sneha", ("Delhi", "Delhi", 110001))
]

employees_df = spark.createDataFrame(
    data,
    ["employee_id","name","address"]
)

"""
employees_df.select(
    "name",
    "address.city"
).show(truncate=False)

Without explicit schema it will throw the following error. 
File "C:\python\Lib\site-packages\pyspark\errors\exceptions\captured.py", line 185, in deco
    raise converted from None
pyspark.errors.exceptions.captured.AnalysisException: [FIELD_NOT_FOUND] No such struct field `city` in `_1`, `_2`, `_3`.
"""

employees_df.select(
    "name",
    "address._1"
).show(truncate=False)

"""
With explicit schema
"""

schema = StructType([
    StructField("employee_id",IntegerType()),
    StructField("name",StringType()),
    StructField("address",
                StructType([
                    StructField("city",StringType()),
                    StructField("state",StringType()),
                    StructField("pincode",IntegerType())
                ])
                )
])

"""
Task 1: Extract the city into a new column called city.

Task 2: Extract the state into a new column called state.

Task 3: Extract the pincode into a new column called pincode.

Task 4: Filter employees whose city is "Pune".
"""

employees_df = spark.createDataFrame(data, schema)

employees_df = employees_df.withColumn(
    "city",
    F.col("address.city")
).withColumn(
    "state",
    F.col("address.state")
).withColumn(
    "pincode",
    F.col("address.pincode")
)

employees_df.show(truncate=False)
employees_df.filter(F.col("city") == "Pune").show(truncate=False)