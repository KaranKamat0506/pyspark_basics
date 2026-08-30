from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

customers = [
    (1,"Rahul"),
    (2,"Amit"),
    (3,"Priya"),
    (4,"John")
]

orders = [
    (101,1,500),
    (102,2,700),
    (103,1,300)
]

customers_df = spark.createDataFrame(customers, ["customer_id","name"])
orders_df = spark.createDataFrame(orders, ["order_id","customer_id","amount"])

"""
Find customers who have placed at least one order.

Expected:

1 | Rahul
2 | Amit

Hint: You don't need any columns from orders_df.
"""

result_df = customers_df.join(
    orders_df,
    customers_df.customer_id == orders_df.customer_id,
    "left_semi"
).select(customers_df.customer_id, customers_df.name)
result_df.show()

result_df_2 = customers_df.join(
    orders_df,
    customers_df.customer_id == orders_df.customer_id,
    "left_anti"
).select(customers_df.customer_id, customers_df.name)
result_df_2.show()

