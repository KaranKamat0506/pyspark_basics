from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

employee_data = [
    (1, "Rahul", "IT", 50000),
    (2, "Priya", "HR", 65000),
    (3, "Amit", "IT", 75000),
    (4, "Sneha", "Finance", 55000),
    (5, "Neha", "HR", 80000),
    (6, "Vikas", "Finance", 60000),
    (7, "Rohit", "IT", 90000),
    (8, "Anjali", "HR", 70000),
    (9, "Karan", "Finance", 85000),
    (10, "Pooja", "IT", 65000)
]

columns = ["employee_id", "name", "department", "salary"]

employees_df = spark.createDataFrame(employee_data,columns)

window_spec = (
    Window.partitionBy("department")
    .orderBy(
        F.col("salary").desc(),
        F.col("employee_id").asc()
    )
)

running_window = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)

result_df = employees_df.withColumn(
    "salary_rank",
    F.dense_rank().over(window_spec)
).withColumn(
    "previous_salary",
    F.lag("salary",1).over(window_spec)
).withColumn(
    "next_salary",
    F.lead("salary",1).over(window_spec)
).withColumn(
    "running_total",
    F.sum("salary").over(running_window)
).withColumn(
    "avg_dept_salary",
    F.avg("salary").over(Window.partitionBy("department"))
).withColumn(
    "max_salary",
    F.max("salary").over(Window.partitionBy("department"))
).withColumn(
    "min_salary",
    F.min("salary").over(Window.partitionBy("department"))
)

result_df.orderBy("department","salary_rank").show()

