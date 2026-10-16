from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("Joins").getOrCreate()

employee = [
    (1,"Rahul"),
    (2,"Amit"),
    (3,"John"),
]

salary = [
    (1,50000),
    (1,55000),
    (2,60000),
    (3,70000)
]

emp_df = spark.createDataFrame(employee,["emp_id","name"])
salary_df = spark.createDataFrame(salary,["emp_id","salary"])

result_df = emp_df.join(salary_df,emp_df.emp_id == salary_df.emp_id)
result_df.show()

window_spec = Window.partitionBy("emp_id").orderBy("salary".desc())

salary_ranked_df = salary_df.withColumn(
    "rn",
    row_number().over(window_spec)
)
salary_ranked_df.show()

latest_salary_df = salary_ranked_df.filter("rn"==1).drop("rn")
latest_salary_df.show()

result_df=emp_df.join(
    latest_salary_df,
    emp_df.emp_id == latest_salary_df.emp_id,
    "inner"
)
result_df.show()
