from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

data=[
     ("Rohan", 30, "Pune"),
    ("Aman", 25, "Mumbai"),
    ("Neha", 35, "Delhi"),
    ("Raj", 28, "Bangalore"),
    ("Rahul", 29, "Thane")
]

df=spark.createDataFrame(data,["name","age","city"])

df.write.mode("overwrite").parquet("people_parquet")

df_parquet=spark.read.parquet("people_parquet")

filtered_df=df_parquet.filter("age > 30")
filtered_df.show()
filtered_df.explain(True)