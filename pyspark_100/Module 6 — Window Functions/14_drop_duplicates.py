from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

customer_data = [
    (101, "Rahul", "rahul_old@gmail.com", "2026-11-01 09:00:00"),
    (101, "Rahul", "rahul_new@gmail.com", "2026-11-05 10:30:00"),
    (102, "Priya", "priya_old@gmail.com", "2026-11-02 11:00:00"),
    (102, "Priya", "priya_new@gmail.com", "2026-11-06 14:00:00"),
    (103, "Amit", "amit@gmail.com", "2026-11-03 12:00:00"),
    (104, "Sneha", "sneha@gmail.com", "2026-11-04 15:00:00"),
    (104, "Sneha", "sneha_updated@gmail.com", "2026-11-07 16:00:00")
]

customer_columns = [
    "customer_id",
    "name",
    "email",
    "updated_at"
]

customer_df = spark.createDataFrame(
    customer_data,
    customer_columns
)

customer_df = customer_df.withColumn(
    "updated_at",
    F.to_timestamp("updated_at")
)

"""
Practice Problem 64
Using the same customer_df:

Find the earliest customer record for each customer instead of the latest.

Requirements:
1. Partition by customer_id.
2. Sort updated_at in ascending order.
3. Generate row_num using F.row_number().
4. Filter where row_num == 1.
5. Display customer_id, name, email, and updated_at.
6. Sort by customer_id.
"""

window_spec = Window.partitionBy("customer_id") \
    .orderBy(F.col("updated_at"))

result_df = customer_df.withColumn(
    "rank",
    F.row_number().over(window_spec)
).filter(F.col("rank") == 1)

result_df.select("customer_id","name","email","updated_at").orderBy("customer_id").show()