# 6. Practice exercise
# Using employees_test_df and departments_df:
# 1. Perform a Left Anti Join on department.
# 2. Select only name and department.
# 3. Sort by name in ascending order.
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

extra_employee_df = spark.createDataFrame(
    [(11,"Arjun","Legal",70000)],
    ["employee_id","name","department","salary"]
)

employees_test_df=employees_df.union(extra_employee_df)

print("Employee Dataframe")
employees_test_df.show()
print("Department Dataframe")
departments_df.show()
