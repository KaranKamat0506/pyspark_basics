from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", "15-07-2022"),
    (2, "Priya", "10-03-2023"),
    (3, "Amit", "25-11-2021")
]

columns = ["emp_id", "emp_name", "joining_date"]

df = spark.createDataFrame(data, columns)
df.printSchema()
df = df.withColumn("joining_date",to_date("joining_date","yyyy-MM-dd"))
df.printSchema()

