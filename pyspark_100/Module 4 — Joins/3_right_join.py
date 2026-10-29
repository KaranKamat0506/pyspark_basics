# 6. Quick practice
# Using the existing DataFrames:
# 1. Perform a Right Join.
# 2. Display only department, manager, and name.
# 3. Sort by department in ascending order.
# 4. Display the result.

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

right_join = employees_df.join(
    departments_df,
    on = "department",
    how="right"
)

result = right_join.select("department","manager","name").orderBy("department",ascending=True)
result.show()
