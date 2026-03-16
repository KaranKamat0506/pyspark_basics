from pyspark.sql import SparkSession

# spark=SparkSession.builder.config("spark.hadoop.io.native.lib.available","false").getOrCreate()
spark = SparkSession.builder \
    .appName("SparkTest") \
    .getOrCreate()

data=[
     ("Rohan", 30, "Pune"),
    ("Aman", 25, "Mumbai"),
    ("Neha", 35, "Delhi"),
    ("Raj", 28, "Bangalore"),
    ("Rahul", 29, "Thane")
]

df=spark.createDataFrame(data,["name","age","city"])

#write data as parquet
df.write.mode("overwrite").parquet("people_parquet")
print("Parquet file written successfully")

#read data from parquet file
df2=spark.read.parquet("people_parquet")
df2.show()

