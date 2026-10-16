from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import dense_rank, col

spark = SparkSession.builder.getOrCreate()

data = [
    ("IT", "Amit", 70000),
    ("IT", "Raj", 85000),
    ("IT", "Priya", 85000),
    ("IT", "Neha", 60000),
    ("HR", "Suresh", 50000),
    ("HR", "Kavita", 65000),
    ("HR", "Rohit", 65000),
]
emp = spark.createDataFrame(data, ["dept", "name", "salary"])
emp.show()

window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())
result = emp.withColumn("rnk", dense_rank().over(window_spec))\
          .filter(col("rnk") == 2)\
          .drop("rnk")
result.show()

