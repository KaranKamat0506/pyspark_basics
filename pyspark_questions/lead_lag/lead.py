from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import to_timestamp, col, lead
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, LongType

spark=SparkSession.builder.getOrCreate()

data=[
    (101, "2024-01-01 10:00:00", 'LOGIN'),
    (101,  "2024-01-01 10:05:00", 'VIEW_PRODUCT'),
    (101,  "2024-01-01 10:15:00", 'ADD_TO_CART'),
    (101,  "2024-01-01 10:30:00", 'LOGOUT'),
    (102,  "2024-01-01 11:00:00", 'LOGIN'),
    (102,  "2024-01-01 11:10:00", 'VIEW_PRODUCT'),
    (102,  "2024-01-01 11:40:00", 'LOGOUT')
]

schema = StructType([
    StructField('user_id', LongType(), True),
    StructField("event_time",StringType(), True),
    StructField("event_name", StringType(), True),
])

df = spark.createDataFrame(data, schema=schema)
df=df.withColumn("event_time",to_timestamp(col("event_time"),"yyyy-MM-dd HH:mm:ss"))
df.show()
df.printSchema()

#time gap between events for a particular user
# Find next event type
window_spec=Window.partitionBy("user_id").orderBy("event_time")

df_lead=df.withColumn("next_event",lead(col("event_name")).over(window_spec))
df_lead.show()
df_lead.printSchema()

#Find time difference between event activities
df_lead_time_gap=df.withColumn("time_gap",lead(col("event_time")).over(window_spec)-col("event_time"))
df_lead_time_gap.show()
df_lead_time_gap.printSchema()
