from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", "50000"),
    (2, "Priya", "65000"),
    (3, "Amit", "45000")
]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)
df.printSchema()
df = df.withColumn("salary",col("salary").cast("integer"))
df.printSchema()
