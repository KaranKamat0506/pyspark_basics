from pyspark.sql import SparkSession



spark=SparkSession.builder.enableHiveSupport().getOrCreate()

data=[(101, "USA", 200),
      (102, "USA", 200),
      (103, "India",150),
      (104, "India", 400),
      (105, "Canada", 250)]

columns = ["customer_id","country","amount"]

df = spark.createDataFrame(data, columns)
df.show()

df.write.mode("overwrite")\
    .partitionBy("country")\
    .parquet("/data/partitioned_sales")

df_partition=spark.read.parquet("/data/partitioned_sales")
df_partition.show()

df.write\
    .mode("overwrite")\
    .bucketBy(3,"customer_id")\
    .sortBy("customer_id")\
    .saveAsTable("bucketed_sales")

df_bucket=spark.table("bucketed_sales")
df_bucket.show()


