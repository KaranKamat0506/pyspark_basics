# Your practice exercise
# Try this yourself before moving to Problem 33.
#
# Task: Modify the inner join so that the final DataFrame contains only:
# - name
# - department
# - manager
# Then sort the results by name in ascending order and display them.
#
# Hints:
# - Use join().
# - Use select().
# - Use orderBy() with ascending order.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Employee dataset
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

employee_columns = [
    "employee_id", "name", "department", "salary"
]

employees_df = spark.createDataFrame(
    employee_data, employee_columns
)

# Department dataset
department_data = [
    ("IT", "Ramesh", "Pune"),
    ("HR", "Meera", "Mumbai"),
    ("Finance", "Suresh", "Bengaluru"),
    ("Marketing", "Kavita", "Delhi")
]

department_columns = ["department", "manager", "location"]

departments_df = spark.createDataFrame(
    department_data, department_columns
)

# inner_join_df = employees_df.join(departments_df,
#                                   on = "department",
#                                   how="inner")
#
# result = inner_join_df.select(
#     "employee_id",
#     "name",
#     "department",
#     "salary",
#     "manager",
#     "location"
# )
#
# result.orderBy("employee_id").show()

#syntax
"""
Joining Syntax 
left_df.join(
    right_df, -> this should not be string type this should be attribute
    on = joining_column,
    how = "type of join"
)
"""

#practice problem
inner_join_df = employees_df.join(
    departments_df,
    on = "department",
    how="inner"
)

result = inner_join_df.select("name","department","manager")
result.orderBy(F.desc("name")).show()
