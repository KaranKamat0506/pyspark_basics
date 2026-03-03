from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("Collect").getOrCreate()

data=[
    (30,"Max"),
    (31,"Max"),
    (32,"Max")
      ]
df=spark.createDataFrame(data,["roll_no","name"])

result=df.groupBy("Name")\
    .agg(
    F.concat_ws(",", F.collect_list("roll_no")).alias("roll_no")
)
result.show(truncate=False)