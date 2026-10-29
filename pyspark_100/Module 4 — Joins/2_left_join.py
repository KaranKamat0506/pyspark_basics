# Quick practice
# Using employees_test_df and departments_df:
#
# Write a Left Join that displays only:
# - name
# - department
# - manager
# Sort by name in ascending order.

from pyspark.sql import SparkSession

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

left_join_df = employees_df.join(
    departments_df,
    on="department",
    how="left"
)

left_join_df= left_join_df.select("name","department","manager","location")
left_join_df.show()
