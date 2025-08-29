'''🔹 Problem Statement

You are given the dataset of Test cricket matches with the number of runs scored each day.
data = [
 ("Ind vs Aus", "Day 1", 250),
 ("Ind vs Aus", "Day 2", 310),
 ("Ind vs Aus", "Day 3", 180),
 ("Ind vs Aus", "Day 4", 200),
 ("Eng vs SA", "Day 1", 280),
 ("Eng vs SA", "Day 2", 340),
 ("Eng vs SA", "Day 3", 150),
]
columns = ["match", "day", "runs"]
🔹 Task

For each match, find the next day’s runs using lead and calculate the difference in runs between today and the next day.

🔹 Expected Output
+---------+-----+----+---------+---------+
| match   | day |runs|next_runs|diff     |
+---------+-----+----+---------+---------+
|Ind vs Aus|Day 1|250 |310      |60       |
|Ind vs Aus|Day 2|310 |180      |-130     |
|Ind vs Aus|Day 3|180 |200      |20       |
|Ind vs Aus|Day 4|200 |null     |null     |
|Eng vs SA |Day 1|280 |340      |60       |
|Eng vs SA |Day 2|340 |150      |-190     |
|Eng vs SA |Day 3|150 |null     |null     |
+---------+-----+----+---------+---------+

'''

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lead, col

from pyspark_questions.pysparkq3 import window_spec

spark = SparkSession.builder.appName("test").getOrCreate()

data = [
 ("Ind vs Aus", "Day 1", 250),
 ("Ind vs Aus", "Day 2", 310),
 ("Ind vs Aus", "Day 3", 180),
 ("Ind vs Aus", "Day 4", 200),
 ("Eng vs SA", "Day 1", 280),
 ("Eng vs SA", "Day 2", 340),
 ("Eng vs SA", "Day 3", 150),
]
columns = ["match", "day", "runs"]

df = spark.createDataFrame(data,columns)
df.show()

window_spec=Window.partitionBy("match").orderBy("day")

df_next_runs=df.withColumn("next_runs",lead("runs").over(window_spec))
df_next_runs.show()

df_diff=df_next_runs.withColumn("diff",col("next_runs")-col("runs"))
df_diff.show()