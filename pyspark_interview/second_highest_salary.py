from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    (101,"Rahul","IT",90000),
    (102,"Priya","HR",75000),
    (103,"Amit","IT",85000),
    (104,"Sneha","Finance",60000),
    (105,"Neha","HR",90000),
    (106,"Vikas","Finance",70000),
    (107,"Rohit","IT",80000),
    (108,"Anjali","HR",65000),
    (109,"Karan","Finance",85000),
    (110,"Pooja","IT",55000)
]

schema = ["employee_id","name","department","salary"]

df = spark.createDataFrame(data,schema)
df.show()

window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())

result_df = df.withColumn(
    "rnk",
    F.dense_rank().over(window_spec)
)

result_df.filter(F.col("rnk") == 2).show(truncate=False)
