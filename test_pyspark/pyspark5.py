'''🔹 Problem Statement

You are given a dataset containing the train departure schedule from a station.
data = [
 ("TrainA", "2024-01-01 09:00"),
 ("TrainB", "2024-01-01 11:00"),
 ("TrainC", "2024-01-01 13:30"),
 ("TrainD", "2024-01-01 16:00"),
]
columns = ["train", "departure_time"]

🔹 Task

For each train, find the next train’s departure time and calculate the waiting time (in minutes) until the next train.

🔹 Expected Output

+-------+----------------+----------------+-----------+
| train | departure_time | next_departure | wait_mins |
+-------+----------------+----------------+-----------+
| TrainA| 2024-01-01 09:00| 2024-01-01 11:00|       120 |
| TrainB| 2024-01-01 11:00| 2024-01-01 13:30|       150 |
| TrainC| 2024-01-01 13:30| 2024-01-01 16:00|       150 |
| TrainD| 2024-01-01 16:00|            null|       null |
+-------+----------------+----------------+-----------+


'''


from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lead, col, unix_timestamp

spark=SparkSession.builder.getOrCreate()

data = [
 ("TrainA", "2024-01-01 09:00"),
 ("TrainB", "2024-01-01 11:00"),
 ("TrainC", "2024-01-01 13:30"),
 ("TrainD", "2024-01-01 16:00"),
]
columns = ["train", "departure_time"]

df=spark.createDataFrame(data, columns)
df=df.withColumn("departure_time", col("departure_time").cast("timestamp"))
df.show()

window_spec=Window.orderBy(df.departure_time)

df_next_depart=df.withColumn("next_departure",lead("departure_time").over(window_spec))
df_next_depart.show()

df_wait_time=df_next_depart.withColumn("wait_mins",(unix_timestamp("next_departure")-unix_timestamp("departure_time"))/60)
df_wait_time.show(truncate=False)