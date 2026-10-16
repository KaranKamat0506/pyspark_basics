from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data = [
    (1,"Laptop",55000,"Electronics"),
    (2,"Mouse",15000,"Accessories"),
    (3,"Keyboard", 1500,"Accessories"),
    (4,"Monitor",12000,"Electronics")
]

columns = ["product_id","product_name","price","category"]

df = spark.createDataFrame(data, columns)
df.select("product_name","price").show()
