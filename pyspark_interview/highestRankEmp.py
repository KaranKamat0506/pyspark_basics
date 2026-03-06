from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, dense_rank, row_number

spark=SparkSession.builder.getOrCreate()

data=[
    (1, "HR", 50000),
    (2, "HR", 60000),
    (3, "HR", 60000),
    (4, "IT", 80000),
    (5, "IT", 90000),
    (6, "IT", 90000)
]

columns=["emp_id","dept","salary"]

df=spark.createDataFrame(data,columns)
df.show(truncate=False)
df.printSchema()

window_spec=Window.partitionBy("dept").orderBy(col("salary").desc())

result=df.withColumn("rnk",dense_rank().over(window_spec)) \
    .filter(col("rnk")==1)\
    .select("emp_id","dept","salary")
result.show(truncate=False)

#If the requirement is to return only one employee per department, I would use row_number().
#If the requirement is to return all employees sharing the highest salary, I would use rank() or dense_rank().

result=df.withColumn("rnk",row_number().over(window_spec))\
    .filter(col("rnk")==1)\
    .select("emp_id","dept","salary")
result.show(truncate=False)
