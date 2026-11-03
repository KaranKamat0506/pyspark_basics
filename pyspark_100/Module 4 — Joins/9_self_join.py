# Problem 40 — Self Join: Practice Exercise
# 1. Perform a Self Join on employees_df to retrieve each employee's manager name.
# 2. Use a Left Join so employees without managers are also included.
# 3. Select employee_id, employee_name, and manager_name.
# 4. Sort by employee_id in ascending order.
# 5. Display the result.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

employee_data = [
    (1, "Rahul", None),
    (2, "Priya", 1),
    (3, "Amit", 1),
    (4, "Sneha", 2),
    (5, "Neha", 2)
]

columns = ["employee_id", "name", "manager_id"]

employees_df = spark.createDataFrame(
    employee_data, columns
)

emp = employees_df.alias("emp")
mgr = employees_df.alias("mgr")

result = emp.join(
    mgr,
    F.col("emp.manager_id") == F.col("mgr.employee_id"),
    how="inner"
)

result.show()
