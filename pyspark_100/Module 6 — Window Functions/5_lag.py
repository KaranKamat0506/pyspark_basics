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

window_spec = Window.partitionBy("user_id").orderBy("login_time")

result_df = login_df.withColumn(
    "previous_login",
    F.lag("login_time",1).over(window_spec)
)

result_df = result_df.withColumn(
    "minutes_since_previous_login",
    (F.col("login_time").cast("long") - F.col("previous_login").cast("long")) / 60
)

result_df.show()


