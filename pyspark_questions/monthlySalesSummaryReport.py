"""
Question :

You have a PySpark DataFrame `sales_data` with the following schema:

| Column Name | Type |
| -------------- | ------------------- |
| `region` | string |
| `product` | string |
| `sales_amount` | double |
| `sales_date` | string (yyyy-MM-dd) |

Scenario:

Your manager wants you to generate a monthly sales summary report with the following requirements:

1. Convert `sales_date` to proper DateType.
2. Extract `year` and `month`.
3. Calculate total_sales, average_sales, and number_of_orders per `region`, `product`, `year`, and `month`.
4. Output the result sorted by `region`, `year`, and `month`.

"""



from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark=SparkSession.builder.getOrCreate()

sales_data=spark.read.format("csv").option("header","true").load("sales.csv")
sales_data.printSchema()

df=sales_data.withColumn("sales_date",F.to_date("sales_date","yyyy-MM-dd"))

df=df.withColumn("year",F.year(F.col("sales_date")))\
    .withColumn("month",F.month(F.col("sales_date")))

monthly_summary=(
    df.groupBy("region","product","year","month")
    .agg(
        F.sum("sales_amount").alias("total_sales"),
        F.avg("sales_amount").alias("avg_sales"),
        F.count("*").alias("number_of_orders")
    )
)

final_report=monthly_summary.orderBy("region","year","month")
final_report.show()