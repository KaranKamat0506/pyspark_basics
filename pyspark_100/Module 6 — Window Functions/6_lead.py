from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

login_data = [
    (101, "2026-11-08 09:00:00"),
    (101, "2026-11-08 10:30:00"),
    (101, "2026-11-08 14:00:00"),
    (102, "2026-11-08 08:45:00"),
    (102, "2026-11-08 12:00:00"),
    (102, "2026-11-08 18:30:00")
]

columns = ["user_id", "login_time"]

login_df = spark.createDataFrame(login_data, columns)

login_df = login_df.withColumn(
    "login_time",
    F.to_timestamp("login_time")
)

"""
Using the same login_df:
1. Partition by user_id.
2. Order by login_time ascending.
3. Use F.lead() with offset 2.
4. Create a column named login_after_two.
5. Display user_id, login_time, and login_after_two.
6. Sort by user_id and login_time.
"""


window_spec = Window.partitionBy("user_id").orderBy("login_time")

result_df = login_df.withColumn(
    "login_after_two",
    F.lead("login_time",2).over(window_spec)
)

result_df = result_df.withColumn(
    "minutes_of_login_after_two",
    (F.col("login_after_two").cast("long") - F.col("login_time").cast("long")) / 60
)

result_df.select("user_id","login_time","login_after_two").orderBy("user_id","login_time").show()