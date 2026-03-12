from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField

spark=SparkSession.builder.getOrCreate()

df=spark.read.json("data.json")
df.show(truncate=False)
#This will print output like this:
'''
+---+-----+
|age|name |
+---+-----+
|30 |Rohan|
|25 |Aman |
+---+-----+
'''

#to follow schema we can do this

schema=StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True)
])

df=spark.read.schema(schema).json("data.json")
df.show(truncate=False)

#Reading multline json
df=spark.read.option("multiline","true").json("multilineJson.json")
df.show(truncate=False)
