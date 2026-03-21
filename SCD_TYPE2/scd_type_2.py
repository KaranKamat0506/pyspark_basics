#1.Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

#2.Creating Spark Session
spark=SparkSession.builder.getOrCreate()

#3. Existing Dimension Table
existing_data=[
    (101,"John","New York","2024-01-01",None,"Y"),
    (102,"Lisa","Texas","2024-01-01",None,"Y")
]

columns_existing=StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("city",StringType(),True),
    StructField("start_date",StringType(),True),
    StructField("end_date",StringType(),True),
    StructField("current_flag",StringType(),True)
])

df_existing=spark.createDataFrame(existing_data,columns_existing)
print("Existing Data")
df_existing.show()

#4. New Source Data
new_data=[
    (101,"John","Chicago"),
    (103,"Sam","Chicago")
]

columns_new=["customer_id","name","city"]

df_new=spark.createDataFrame(new_data,columns_new)
print("New Data")
df_new.show()

#5. Join source with existing
df_join=df_existing.alias("t").join(
    df_new.alias("s"),
    col("t.customer_id") == col("s.customer_id"),
    "left"
)
print("Join Data")
df_join.show()

#6. Detect Changed Records
df_changed=df_join.filter(
    (col("t.city") != col("s.city")) &col("s.city").isNotNull()
).drop(col("s.customer_id"))
print("Changed Data")
df_changed.show()

#7. Expire Old Records
df_expired=df_existing.join(
    df_changed.select("customer_id"),"customer_id",how="left_semi")\
    .withColumn("end_date",current_date())\
    .withColumn("current_flag",lit("N"))

print("Expired Data")
df_expired.show()

#8. Insert New Records
df_insert=df_new.withColumn(
    "start_date",current_date()
)\
    .withColumn("end_date",current_date()) .withColumn("current_flag",lit("Y"))

#9. Keep unchanged records
df_unchanged=df_existing.join(
    df_changed.select("customer_id"),"customer_id",how="left_anti"
)

#10.Final dimension table
df_final=df_unchanged.unionByName(df_expired).unionByName(df_insert)
df_final.show()
