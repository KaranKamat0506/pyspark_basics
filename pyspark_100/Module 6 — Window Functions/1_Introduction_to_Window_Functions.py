from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

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

employees_df.show()

# Calculate the average salary by department

window_spec = Window.partitionBy("department")

result = employees_df.withColumn(
    "avg_salary",
    F.avg("salary").over(window_spec)
)

result.show()

"""
Your practice — Problem 51
Use the same employees_df DataFrame.

Task: Create a new column called max_department_salary that displays the highest salary in each employee's department.

Requirements:
1. Define a window using Window.partitionBy("department").
2. Use F.max("salary").
3. Apply the window using .over().
4. Display name, department, salary, and max_department_salary.
"""

window_spec = Window.partitionBy("department")

result_df_2 = employees_df.withColumn(
    "max_salary_department",
    F.max("salary").over(window_spec)
)

result_df_2.select("name","department","salary","max_salary_department").show()
