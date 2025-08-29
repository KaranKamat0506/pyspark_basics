'''
𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧:

You are given an orders dataset containing information about customer purchases.
Each order can contain multiple items stored in a single column as an array.

Input Dataset:

order_id customer items
 1 Aarav   ["mango", "banana", "guava"]
 2 Priya    ["apple", "lychee"]
 3 Rohan   ["papaya"]

𝐬𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐝𝐚𝐭𝐚𝐬𝐞𝐭

data = [
 (1, "Aarav", ["mango", "banana", "guava"]),
 (2, "Priya", ["apple", "lychee"]),
 (3, "Rohan", ["papaya"])
]
columns = ["order_id", "customer", "items"]

df = spark.createDataFrame(data, columns)

𝐄𝐱𝐩𝐞𝐜𝐭𝐞𝐝 𝐨𝐮𝐭𝐩𝐮𝐭:

+--------+--------+------+
|order_id|customer| item|
+--------+--------+------+
|    1|  Aarav| mango|
|    1|  Aarav|banana|
|    1|  Aarav| guava|
|    2|  Priya| apple|
|    2|  Priya|lychee|
|    3|  Rohan|papaya|
+--------+--------+------+
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col

spark=SparkSession.builder.getOrCreate()

data = [
 (1, "Aarav", ["mango", "banana", "guava"]),
 (2, "Priya", ["apple", "lychee"]),
 (3, "Rohan", ["papaya"])
]

columns = ["order_id", "customer", "items"]

df=spark.createDataFrame(data, columns)
print("Original Data")
df.show()

df_explode=df.withColumn("items",explode(col("items")))
df_explode.show()



