from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col

spark=SparkSession.builder.getOrCreate()

data=[("Laptop",800),("Mouse",25),("Keyboard",150),("Monitor",300)]
columns=["product","price"]

df=spark.createDataFrame(data,columns)
df.show()

df_price_category= df.withColumn("price_category",
                                 when(col("price") < 100,"Low")
                                 .when((col("price") >= 100) & (col("price") < 500),"Medium")
                                 .otherwise("High")
                                 )

df_price_category.show()