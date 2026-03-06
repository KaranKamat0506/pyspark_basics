from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, dense_rank

spark = SparkSession.builder.getOrCreate()

data=[
    (1,"HR",50000),
    (2,"HR",60000),
    (3,"HR",60000),
    (4,"IT",80000),
    (5,"IT",90000),
    (6,"IT",90000)
    ]
columns=["emp_id","dept","salary"]

df=spark.createDataFrame(data,columns)
df.show()

window_spec=Window.partitionBy("dept").orderBy(col("salary").desc())
result=df.withColumn("rnk",dense_rank().over(window_spec)) \
    .filter(col("rnk")==2) \
    .select("emp_id","dept","salary")
result.show()