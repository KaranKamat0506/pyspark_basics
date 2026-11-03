# Problem 39 — Handle Duplicate Column Names: Practice Exercise
# 1. Join employees_df and departments_df using an explicit join condition on department.
# 2. Use aliases to distinguish the two DataFrames.
# 3. Select employee_id, name, department, manager, and location.
# 4. Sort by employee_id in ascending order.
# 5. Display the result.


#handling duplicate column names
#1. Simple using on = "column_name", considering column_name is common in both the dataframes
#2. Renaming one of the columns
#3. Using alias


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

employee_data = [
(1, "Rahul", "IT", "Pune"),
    (2, "Priya", "HR", "Mumbai"),
    (3, "Amit", "IT", "Mumbai"),
    (4, "Sneha", "Finance", "Bengaluru"),
    (5, "Neha", "HR", "Pune")
]

employee_columns = ["employee_id","name","department","location"]
employee_df = spark.createDataFrame(employee_data, employee_columns)

department_data = [
    ("IT","Pune","Ramesh"),
    ("HR","Mumbai","Meera"),
    ("IT","Bengaluru","Suresh"),
    ("Finance","Bengaluru","Kavita")
]

department_columns = ["department","location","manager"]

department_df = spark.createDataFrame(department_data,department_columns)

# result_df = employee_df.join(
#     department_df,
#     on="department",
#     how="inner"
# )
#
# emp = employee_df.alias("emp")
# dept = department_df.alias("department")
#
# joined_df = emp.join(
#     dept,
#     F.col("emp.deparment") == F.col("dept.department"),
#     how="inner"
# )
#
# departments_renamed_df = department_df.withColumnRenamed("department","dept_name")
# result = employee_df.join(
#     departments_renamed_df,
#     employee_df.department == departments_renamed_df.dept_name,
#     "inner"
# )

e = employee_df.alias("e")
d = department_df.alias("d")

result_df=e.join(
    d,
    F.col("e.department") == F.col("d.department"),
    "inner"
)
result_df = result_df.select("e.employee_id","e.name","e.department","d.manager","d.location")
result_df.show()
