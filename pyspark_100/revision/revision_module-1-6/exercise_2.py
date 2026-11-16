from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, current_date
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

employee_data = [
    (1, " Rahul ", "IT", "50000", "2022-01-10"),
    (2, "PRIYA", "HR", "65000", "2021-05-15"),
    (3, "Amit", "IT", "75000", "2023-03-20"),
    (4, "Sneha ", "Finance", "55000", "2022-08-11"),
    (5, "Neha", "HR", "80000", "2020-12-01"),
    (6, "Vikas", "Finance", None, "2021-07-19"),
    (7, "Rohit", "IT", "90000", "2019-04-25"),
    (8, "Anjali", "HR", "70000", None),
    (9, "Karan", "Finance", "85000", "2020-09-12"),
    (10, "Pooja", "IT", "65000", "2023-06-01")
]

columns = [
    "employee_id", "name", "department",
    "salary", "joining_date"
]

employees_df = spark.createDataFrame(employee_data, columns)

department_data = [
    ("IT", "Technology", "Mumbai"),
    ("HR", "Human Resources", "Pune"),
    ("Finance", "Accounts", "Bengaluru"),
    ("Marketing", "Marketing", "Delhi")
]

department_columns = [
    "department",
    "department_name",
    "location"
]

departments_df = spark.createDataFrame(
    department_data,
    department_columns
)

"""
Your tasks
Task 1 — Inner Join

Perform an Inner Join between employees_df and departments_df using the department column.

Task 2 — Left Join

Perform a Left Join and identify employees whose department information doesn't match.

Task 3 — Left Anti Join

Find employees whose department does not exist in the department DataFrame.

Task 4 — Left Semi Join

Find employees whose department exists in the department DataFrame.

Task 5 — Handle duplicate columns

After joining, make sure you don't have duplicate department columns.

"""

#task 1
employees_df.join(
    departments_df,
    on="department",
    how = "inner"
).select("name","department","location").show()


#task2
result_df = employees_df.join(
    departments_df,
    on="department",
    how = "left"
)

result_df.filter(
    F.col("department_name").isNull()
).show()

#task3
employees_df.join(
    departments_df,
    on="department",
    how = "left_anti"
).select("name","department").show()

#task4
employees_df.join(
    departments_df,
    on="department",
    how="left_semi"
).select("name","department").show()

#task5
e = employees_df.alias("e")
d = departments_df.alias("d")

e.join(
    d,
    F.col("e.department") == F.col("d.department"),
    how = "inner"
).select("e.name","e.department","d.department","d.department_name","d.location").show()


"""
Task 6 — Select Specific Columns
Using your joined DataFrame, display only:
- name
- department_name
- location
"""

print("task 6")
employees_df.join(
    departments_df,
    on="department",
    how="inner"
).select("name","department_name","location").show()

"""
Task 7 — Employee Count by Location
- Join employees and departments.
- Group employees by location.
- Count the number of employees in each location.
"""

print("Task 7")
result_df = employees_df.join(
    departments_df,
    on="department",
    how="inner"
).groupBy("location").agg(
    F.count("*").alias("employee_per_location")
)
result_df.show()

"""
Task 8 — Average Salary by Location
- Calculate the average salary for each location.
- Display location and average_salary.
"""

print("Task 8")
avg_df = employees_df.join(
    departments_df,
    on="department",
    how = "inner"
).groupBy("location").agg(
    F.avg("salary").alias("average_salary")
)

avg_df.select("location","average_salary").show()

"""
Task 9 — Filter Aggregated Results
- Find locations where the average salary is greater than ₹65,000.
- Use filter() or where().
"""

print("task 9")
avg_df.filter(F.col("average_salary") >= 65000).show()

"""
Task 10 — Sorting
- Sort locations by average salary in descending order.
- Display the highest average salary first.
"""
print("Task 10")
avg_df.orderBy(F.col("average_salary").desc()).show()


