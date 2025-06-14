from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckSetup") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version hello:", spark.version)
# spark.stop()



