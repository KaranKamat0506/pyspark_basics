#rank,dense rank and row
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, dense_rank, rank

spark=SparkSession.builder \
    .appName("PysparkLocalDemo") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ("A", "Maths", 90),
    ("B", "Maths", 95),
    ("C", "Maths", 90),
    ("D", "Maths", 88),
    ("E", "Maths", 95),
]

columns = ["student", "subject", "marks"]

df=spark.createDataFrame(data, columns)
print("Original data")
df.show()

window_spec = Window.partitionBy("subject").orderBy("marks")

df_ranked=df.withColumn("row_num",row_number().over(window_spec))\
    .withColumn("dense_rank",dense_rank().over(window_spec))\
    .withColumn("rank",rank().over(window_spec))

print("Ranked_data")
df_ranked.show()
