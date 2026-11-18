from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark  = SparkSession.builder.getOrCreate()

employee_data = [
    (1, " Rahul ", "IT", "75000", "2022-01-10"),
    (2, "PRIYA", "HR", "65000", "2021-05-15"),
    (3, "Amit", "IT", "90000", "2023-03-20"),
    (4, "Sneha ", "Finance", "55000", "2022-08-11"),
    (5, "Neha", "HR", "85000", "2020-12-01"),
    (6, "Vikas", "Finance", None, "2021-07-19"),
    (7, "Rohit", "IT", "95000", "2019-04-25"),
    (8, "Anjali", "HR", "70000", None),
    (9, "Karan", "Finance", "88000", "2020-09-12"),
    (10, "Pooja", "IT", "65000", "2023-06-01"),
    (11, "Rahul", "IT", "80000", "2024-01-15")
]

employee_columns = [
    "employee_id", "name", "department", "salary", "joining_date"
]

employees_df = spark.createDataFrame(employee_data, employee_columns)
print("Employees Dataframe")
employees_df.show()

department_data = [
    ("IT", "Technology", "Mumbai"),
    ("HR", "Human Resources", "Pune"),
    ("Finance", "Accounts", "Bengaluru"),
    ("Marketing", "Marketing", "Delhi")
]

department_columns = [
    "department", "department_name", "location"
]

departments_df = spark.createDataFrame(
    department_data,
    department_columns
)

print("Department Dataframe")
departments_df.show()

print("Task 1 - Inspect the employee DataFrame: schema, row count and column count.")
employees_df.printSchema()
print(employees_df.count())
print(len(employees_df.columns))

print("Task 2 - Clean employee names and convert salary and joining_date to appropriate data types.")
cleaned_df = employees_df.withColumn(
    "name",
    F.initcap(F.trim("name"))
).withColumn(
    "salary",
    F.col("salary").cast("integer")
).withColumn(
    "joining_date",
    F.to_date(F.col("joining_date"),"yyyy-MM-dd")
)
cleaned_df.show()
cleaned_df.printSchema()

print("Task -3 Handle NULL salary using the average salary of the employee's department.")

window_spec = Window.partitionBy("department")

null_df_handled = cleaned_df.withColumn(
    "avg_salary",
    F.avg(F.col("salary")).over(window_spec)
).withColumn(
    "salary",
    F.coalesce(
        F.col("salary"),
        F.col("avg_salary")
    )
)
null_df_handled.show()

print("Task 4 - Filter employees whose salary is greater than ₹70,000 and belong to IT or HR.")
cleaned_df.filter(
    (
        (F.col("salary") > 70000) & ((F.col("department") == "IT") | (F.col("department") == "HR"))
    )
).show()

print("Task 5 - Join employees with department information and identify employees without matching department information.")
employees_df.join(
    departments_df,
    on = "department",
    how = "left_anti"
).show()

print("Task 6 - Calculate total, average, maximum and minimum salary for each department.")
group_by_df = employees_df.groupBy("department").agg(
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("average_salary"),
    F.max("salary").alias("max_salary"),
    F.min("salary").alias("min_salary")
)
group_by_df.show()

print("Task 7 - Find departments whose average salary is greater than ₹70,000.")
group_by_df.filter(F.col("average_salary") > 70000).show()

print("Task 8 - Rank employees by salary within each department using a window function.")
window_spec_sal = Window.partitionBy("department").orderBy(F.col("salary").desc())

cleaned_df = cleaned_df.withColumn(
    "rnk",
    F.dense_rank().over(window_spec_sal)
)
cleaned_df.show()

print("Task 9 - Retain the top two highest-paid employees in each department.")
cleaned_df.filter(F.col("rnk") >=2 ).show()

print("Task 10 - Calculate each employee's salary difference from their department's average salary.")

window_spec_sal_avg = Window.partitionBy("department")

task10_df = cleaned_df.withColumn(
    "salary_difference",
        F.abs(
            F.avg("salary").over(window_spec_sal_avg) - F.col("salary")
    )
)
task10_df.show()