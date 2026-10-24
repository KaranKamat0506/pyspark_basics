from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1,"Rahul","IT",50000),
    (2,"Priya","HR",65000),
    (3,"Amit","IT",75000),
    (4,"Sneha","Finance",55000),
    (5,"Neha","HR",80000)
]

columns = ["emp_id","emp_name","department","salary"]

df = spark.createDataFrame(data,columns)
df = df.filter(df.salary < 60000)
df.show()