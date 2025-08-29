'''
🔹 Problem Statement

You are given a dataset containing the daily stock closing prices for multiple companies.

Task:
Write a PySpark program to find all days where the stock price decreased compared to the previous day for the same company.

data = [
 ("TCS", "2024-01-01", 3500),
 ("TCS", "2024-01-02", 3550),
 ("TCS", "2024-01-03", 3520),
 ("TCS", "2024-01-04", 3560),
 ("INFY", "2024-01-01", 1500),
 ("INFY", "2024-01-02", 1480),
 ("INFY", "2024-01-03", 1495),
 ("INFY", "2024-01-04", 1470),
]
columns = ["company", "date", "close_price"]

🔹 Expected Output

We want only the days where the stock price fell compared to the previous day.

+-------+----------+-----------+----------+----------+
|company|      date|close_price|prev_price|price_diff|
+-------+----------+-----------+----------+----------+
|    TCS|2024-01-03|       3520|      3550|      -30 |
|   INFY|2024-01-02|       1480|      1500|      -20 |
|   INFY|2024-01-04|       1470|      1495|      -25 |
+-------+----------+-----------+----------+----------+
'''



from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, window, lag

spark=SparkSession.builder.getOrCreate()

data = [
 ("TCS", "2024-01-01", 3500),
 ("TCS", "2024-01-02", 3550),
 ("TCS", "2024-01-03", 3520),
 ("TCS", "2024-01-04", 3560),
 ("INFY", "2024-01-01", 1500),
 ("INFY", "2024-01-02", 1480),
 ("INFY", "2024-01-03", 1495),
 ("INFY", "2024-01-04", 1470),
]
columns = ["company", "date", "close_price"]

df=spark.createDataFrame(data,columns)
df.show()

window_spec=Window.partitionBy("company").orderBy("date")

#column which has previous column value
df_with_prev=df.withColumn("prev_price",lag("close_price").over(window_spec))
df_with_prev.show()

#column containing difference between current days price and previous days price
df_with_diff=df_with_prev.withColumn("price_diff",(col("close_price")-col("prev_price")))
df_with_diff.show()

#filter the data  where the stock price decreased compared to the previous day
df_with_decreased=df_with_diff.filter(col("price_diff")<0).show()
