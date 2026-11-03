# Problem 38 — Join Using Multiple Columns: Practice Exercise
# 1. Perform an Inner Join using both department and location.
# 2. Select name, department, location, and manager.
# 3. Sort by name in ascending order.
# 4. Display the result.

from pyspark.sql import SparkSession

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
#     on=["department","location"],
#     how="left"
# )

result_df = employee_df.join(
    department_df,
    on=["department","location"],
    how="left"
)

result_df = result_df.select("name","department","location","manager").orderBy("name",ascending=True)
result_df.show()
