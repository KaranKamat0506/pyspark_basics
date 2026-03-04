from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

data=[
    (1,"Rohan",25),
    (1,"Rohan",25),
    (2,"Amit",30)
]

schema="roll_no INT,name STRING, age INT"

df=spark.createDataFrame(data,schema=schema)
df.show()
df.dropDuplicates().show()

df.dropDuplicates(["name"]).show()