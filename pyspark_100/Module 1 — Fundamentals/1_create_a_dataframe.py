from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1,"Laptop",55000),
    (2,"Mouse",500),
    (3,"Keyboard",1500)
]

columns = ["product_id","product_name","price"]

df = spark.createDataFrame(data, columns)
df.show()
df.printSchema()
