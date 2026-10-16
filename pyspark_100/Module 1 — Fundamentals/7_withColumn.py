from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", "Pune", 50000),
    (2, "Priya", "Mumbai", 65000),
    (3, "Amit", "Delhi", 45000)
]

columns = ["id", "name", "city", "salary"]

df = spark.createDataFrame(data, columns)
df=df.withColumn("country",lit("India"))
df.show()
