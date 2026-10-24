from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Rahul", "IT", 50000),
    (2, "Priya", "HR", 65000),
    (1, "Rahul", "IT", 50000),
    (3, "Amit", "IT", 75000)
]

columns = ["emp_id", "emp_name", "department", "salary"]

df = spark.createDataFrame(data, columns)

df_clean = df.distinct()
df_clean.show()

df_clean2 = df.dropDuplicates()
df_clean2.show()

data2 = [
    (1, "Rahul", "IT", 50000),
    (2, "Priya", "HR", 65000),
    (1, "Rahul", "IT", 55000),
    (3, "Amit", "IT", 75000)
]

columns2 = ["emp_id", "emp_name", "department", "salary"]

df1 = spark.createDataFrame(data2, columns2)

# detecting duplicate values based on columns
df_clean3 = df1.dropDuplicates(["emp_id"])
df_clean3.show()
