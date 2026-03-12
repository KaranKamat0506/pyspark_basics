from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark=SparkSession.builder.getOrCreate()

df=spark.read.option("multiline","true").json("multilineJson.json")
df.show(truncate=False)

df=df.select(
    "name",
    explode("skills").alias("skill")
)

print("Exploded Data")
df.show(truncate=False)