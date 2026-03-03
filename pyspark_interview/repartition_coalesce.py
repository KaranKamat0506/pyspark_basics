from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.getOrCreate()

flight_df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("C:\\Users\\MY DELL\\Downloads\\2015-summary.csv")

flight_df.show()
flight_df.printSchema()
print(flight_df.count())
print(flight_df.rdd.getNumPartitions())

#current partition is 1
#Now i want to repartition.
partitioned_df=flight_df.repartition(4)
partitioned_df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().show()

"""
Now we can see that records are evenly distributed across partitions.
+-----------+-----+
|partitionId|count|
+-----------+-----+
|          0|   64|
|          1|   64|
|          2|   64|
|          3|   64|
+-----------+-----+
"""
partitioned_on_column=flight_df.repartition(300,"ORIGIN_COUNTRY_NAME")
print(partitioned_on_column.rdd.getNumPartitions())
partitioned_on_column.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().show(300)

"""
+-----------+-----+
|partitionId|count|
+-----------+-----+
|          0|    1|
|          2|    2|
|          7|    1|
|         10|    1|
|         13|    1|
|         15|    2|
|         16|    2|
|         19|    2|
|         22|    1|
|         28|    1|
|         31|    1|
|         39|    1|
|         43|    1|
|         44|    1|
|         45|    2|
|         48|    1|
|         53|    1|
|         55|    1|
|         65|    1|
|         70|    1|
|         73|    2|
|         75|    1|
|         76|    1|
|         81|    1|
|         84|    2|
|         86|    1|
|         87|    1|
|         90|    1|
|         91|    1|
|         97|    2|
|        100|    1|
|        103|    2|
|        104|    1|
|        108|    1|
|        112|    2|
|        115|    1|
|        117|    2|
|        126|    1|
|        127|    2|
|        129|    1|
|        130|    2|
|        132|    1|
|        133|    1|
|        138|    2|
|        144|    1|
|        148|    1|
|        156|    2|
|        157|    1|
|        159|    2|
|        162|    1|
|        165|    1|
|        174|    1|
|        175|    1|
|        178|    1|
|        181|    1|
|        185|    2|
|        188|    1|
|        201|    1|
|        202|    1|
|        204|    2|
|        209|    1|
|        210|    1|
|        213|    2|
|        215|    1|
|        219|    1|
|        220|    1|
|        221|    1|
|        223|    1|
|        226|    1|
|        230|    1|
|        231|    1|
|        238|    2|
|        242|    1|
|        243|    1|
|        245|    1|
|        251|    1|
|        254|    1|
|        256|    1|
|        257|    1|
|        258|    1|
|        259|  133|
|        262|    2|
|        263|    1|
|        267|    1|
|        268|    1|
|        269|    1|
|        271|    1|
|        272|    1|
|        275|    1|
|        276|    1|
|        277|    2|
|        280|    2|
|        281|    1|
|        283|    1|
|        286|    1|
|        288|    1|
|        290|    1|
|        295|    2|
|        296|    1|
|        299|    1|
+-----------+-----+

"""

#Coalesce
print("Coalesce Dataframe")
coalesced_df=flight_df.repartition(8)
print(coalesced_df.rdd.getNumPartitions())
coalesced_df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().show()

three_coalesced_df=coalesced_df.coalesce(3)
print(three_coalesced_df.rdd.getNumPartitions())
three_coalesced_df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().show()

"""
After using repartition 8 it is evenly distributed across partitions.
+-----------+-----+
|partitionId|count|
+-----------+-----+
|          0|   32|
|          1|   32|
|          2|   32|
|          3|   32|
|          4|   32|
|          5|   32|
|          6|   32|
|          7|   32|
+-----------+-----+

After using coalesce uneven distributed across partitions.
+-----------+-----+
+-----------+-----+
|partitionId|count|
+-----------+-----+
|          0|   64|
|          1|   96|
|          2|   96|
+-----------+-----+
"""

print(coalesced_df.coalesce(10).rdd.getNumPartitions())