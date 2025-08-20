'''
Q𝐮𝐞𝐬𝐭𝐢𝐨𝐧:
You are given a dataset containing the daily temperature readings for multiple cities:

Task:
Write a PySpark program to find all days where the temperature increased compared to the previous day for the same city.
𝐬𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐝𝐚𝐭𝐚𝐬𝐞𝐭
data = [
 ("Delhi", "2024-01-01", 15),
 ("Delhi", "2024-01-02", 18),
 ("Delhi", "2024-01-03", 20),
 ("Delhi", "2024-01-04", 19),
 ("Mumbai", "2024-01-01", 28),
 ("Mumbai", "2024-01-02", 27),
 ("Mumbai", "2024-01-03", 30),
]
columns = ["city", "date", "temperature"]
'''


from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

data = [
 ("Delhi", "2024-01-01", 15),
 ("Delhi", "2024-01-02", 18),
 ("Delhi", "2024-01-03", 20),
 ("Delhi", "2024-01-04", 19),
 ("Mumbai", "2024-01-01", 28),
 ("Mumbai", "2024-01-02", 27),
 ("Mumbai", "2024-01-03", 30),
]
columns = ["city", "date", "temperature"]

df=spark.createDataFrame(data=data, schema=columns)
df.show()

window_spec=Window.partitionBy("city").orderBy("date")

df_with_prev=df.withColumn("prev_temp",lag("temperature").over(window_spec))

df_with_diff=df_with_prev.withColumn("temp_diff",(col("temperature")-col("prev_temp")))

#Filter where temperature increased
df_increased=df_with_diff.filter(col("temp_diff")>0).show()

