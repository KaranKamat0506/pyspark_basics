from pandas.core.indexes.multi import sparsify_labels
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", "Pune"),
    (2, "Priya", "Mumbai"),
    (3, "Amit", "Bengaluru"),
    (4, "Sneha", "Pune"),
    (5, "Neha", "Hyderabad")
]

columns = ["emp_id", "emp_name", "city"]

df = spark.createDataFrame(data,columns)
df = df.filter(df.city.isin("Bengaluru","Hyderabad"))
# This is equivalent to
#df = df.filter((col("city") == "Pune") | (col("city") == "Bengaluru"))
# df = df.filter(col("city").isin("Bengaluru","Hyderabad")) -> Using col()
df.show()