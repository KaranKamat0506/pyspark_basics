from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()

schema= "id INT, name STRING, age INT"

df= spark.read \
    .option("header", True)\
    .option("mode","DROPMALFORMED")\
    .schema(schema)\
    .csv("dropMalformed.csv")
df.show()
df.printSchema()