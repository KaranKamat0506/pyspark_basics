from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

employee = [
    ("A","Rahul",10),
    ("B","Amit",20),
    ("C","John",30),
    ("D","Priya",40)
]

department = [
    (10,"IT"),
    (20,"HR"),
    (30,"Finance"),
    (50,"Marketing")
]

employee_df = spark.createDataFrame(employee,["emp_id","name","dept_id"])

department_df = spark.createDataFrame(department,["dept_id","department"])

"""
Write PySpark code to return:
emp_id | name | department
Include only employees whose department exist
Question: Which join will you use?
Inner Join
"""

#Aliases to avoid duplicate column name
emp = employee_df.alias("emp")
dept = department_df.alias("dept")

result_df = emp.join(
    dept,
    emp.dept_id==dept.dept_id,
    "inner"
    ).select(
    emp.emp_id,
    emp.name,
    dept.department
)

result_df.show()

"""
Using the same DataFrames:

Return all employees, even if their department doesn't exist.

Expected:
A | Rahul | IT
B | Amit  | HR
C | John  | Finance
D | Priya | NULL

Question: Which join?
left join
"""

result_df_2 = emp.join(
    dept,
    emp.dept_id==dept.dept_id,
    "left"
).select(emp.emp_id, emp.name,dept.department)
result_df_2.show()

"""
Q3. Find Employees Without a Department

Using the same DataFrames:

Find employees whose dept_id does not exist in department_df.

Expected:

D | Priya | 40

Question: Which join would you use?
"""

result_df_3 = emp.join(
    dept,
    emp.dept_id == dept.dept_id,
    "left_anti"
).select(emp.emp_id, emp.name,emp.dept_id) 
result_df_3.show()

