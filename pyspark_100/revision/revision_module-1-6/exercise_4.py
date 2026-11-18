from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

customer_data = [
    (101, " Rahul ", "rahul_old@gmail.com", "Mumbai", "2026-11-01 09:00:00"),
    (101, "Rahul", "rahul_new@gmail.com", "Pune", "2026-11-05 10:30:00"),
    (102, "PRIYA", "priya_old@gmail.com", "Delhi", "2026-11-02 11:00:00"),
    (102, "Priya", "priya_new@gmail.com", "Mumbai", "2026-11-06 14:00:00"),
    (103, "Amit", None, "Pune", "2026-11-03 12:00:00"),
    (104, "Sneha", "sneha@gmail.com", None, "2026-11-04 15:00:00"),
    (104, "Sneha", "sneha_updated@gmail.com", "Delhi", "2026-11-07 16:00:00"),
    (105, "Karan", "karan@gmail.com", "Mumbai", "2026-11-08 10:00:00")
]

customer_columns = [
    "customer_id", "name", "email", "city", "updated_at"
]

customer_df = spark.createDataFrame(
    customer_data,
    customer_columns
)

customer_df.show()

result_df = customer_df.withColumn(
    "name",
    F.trim(F.col("name"))
).withColumn(
    "name",
    F.initcap(F.col("name"))
)

result_df.select("customer_id","name").show()

print("Task 2 — Convert updated_at into TimestampType.")

result_df = result_df.withColumn(
    "updated_at",
    F.to_timestamp("updated_at","yyyy-MM-dd HH:mm:ss")
)
result_df.select("customer_id","updated_at").show()
result_df.printSchema()

print("Handle NULL email and city values")

result_df.filter(F.col("email").isNull()).show()
result_df.filter(F.col("name").isNull()).show()

result_df = result_df.fillna(
    {"email" : "unknown@gmail.com"},
    {"name" : "Unknown"}
)

result_df.show()

print("Task 4 — Find duplicate customer records")

duplicate_cust_df = result_df.groupBy("customer_id").agg(
    F.count("*").alias("count")
).filter(
    F.col("count") > 1
)
duplicate_cust_df.show()

customer_df.join(
    duplicate_cust_df.select("customer_id"),
    on = "customer_id",
    how="inner"
).show()

print("Task 5 — Retain the latest record for each customer")

cust_window_spec = Window.partitionBy("customer_id") \
    .orderBy(F.col("updated_at").desc())

customer_partitioned_df = result_df.withColumn(
    "row_num",
    F.row_number().over(cust_window_spec)
)
customer_partitioned_df.show()

final_df = customer_partitioned_df.filter(F.col("row_num") == 1)
final_df.show()

print("Task 6 — Find customers updated after 5 November 2026")
final_df.filter(F.col("updated_at") > "2026-11-05").show()

print("Task 7 — Count customers in each city")
grouped_city_df =  result_df.groupBy("city").agg(
    F.count("*").alias("cust_count")
)
grouped_city_df.select("city","cust_count").show()

print("Task 8 — Sort customers by their latest update timestamp")
result_df.orderBy(F.col("updated_at").desc()).show()

print("Task 9 — Calculate the difference in days")

diff_df = result_df.withColumn(
    "days_since_update",
    F.datediff(
        F.to_date(F.lit("2026-11-10")),
        F.to_date(F.col("updated_at"))
    )
)
diff_df.select(
    "customer_id",
    "updated_at",
    "days_since_update"
).show()