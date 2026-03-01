from datetime import date

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import to_timestamp, lag
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, StructField, FloatType

spark=SparkSession.builder.getOrCreate()

data=[
    ('AAPL','2024-01-01',150.00),
    ('AAPL','2024-01-02',152.00),
    ('AAPL','2024-01-03',151.00),
    ('AAPL','2024-01-04',155.00),
    ('AAPL','2024-01-05',158.00),
    ('GOOG','2024-01-01',2700.00),
    ('GOOG','2024-01-02',2725.00),
    ('GOOG','2024-01-03',2710.00),
    ('GOOG','2024-01-04',2735.00),
    ('GOOG','2024-01-05',2750.00)
]

schema=StructType([
    StructField('stock_symbol',StringType(),True),
    StructField('trade_date',StringType(),True),
    StructField('close_price',FloatType(),True)
])

df=spark.createDataFrame(data,schema=schema)
df=df.withColumn("trade_date",to_timestamp(col("trade_date"),"yyyy-mm-dd"))
df.show()
df.printSchema()

window_spec=Window.partitionBy("stock_symbol").orderBy("trade_date")
df_closing_price=df.withColumn("yesterdays_price",lag(col("close_price")).over(window_spec))
df_closing_price.show()