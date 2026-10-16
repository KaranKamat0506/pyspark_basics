from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1,"Laptop",55000),
    (2,"Mouse",500),
    (3,"Keyboard",1500),
    (4,"Monitor",12000),
    (5,"Headphones",2000),
    (6,"Webcam",3000)
]

columns = ["product_id","product_name","price"]

df = spark.createDataFrame(data, columns)
print(df.columns)
df.printSchema()