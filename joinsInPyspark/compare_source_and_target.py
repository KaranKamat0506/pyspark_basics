from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

source = [
    ("A",200),
    ("B",350),
    ("C",400),
    ("E",600)
]

target = [
    ("A",200),
    ("B",300),
    ("C",400),
    ("D",500)
]

source_df = spark.createDataFrame(source,["id","salary"])
target_df = spark.createDataFrame(target,["id","salary"])

"""
Insert
Records present in source but not in target
"""

insert_df = source_df.join(
    target_df,
    source_df.id == target_df.id,
    "left_anti"
)
insert_df.show()

"""
Delete
Records Present in target but not in source
"""

delete_df = target_df.join(
    source_df,
    target_df.id == source_df.id,
    "left_anti"
)
delete_df.show()

"""
Update records where id exists in both but Salary changed
"""
update_df = source_df.join(
    target_df,
    source_df.id == target_df.id,
    "inner"
).filter(
    source_df.salary != target_df.salary
).select(
    source_df.id,source_df.salary
)
update_df.show()

