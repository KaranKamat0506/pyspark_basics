# Practice problem — Module 1
# Using df, write code to:
# 1. Rename name to employee_name.
# 2. Add a column named bonus with a value of 5000 for every employee.
# 3. Select only employee_id, employee_name, salary, and bonus.
# 4. Display the first five rows.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
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

columns = ["employee_id","name","department","salary"]

df = spark.createDataFrame(data, columns)
# df.show() #view records
# df.show(5) # display first 5 rows
# df.printSchema() #Show column names and types
# df.count() #Return number of rows
# print(len(df.columns) )#Return number of columns
# df.select("name","salary") #Choose specific columns
# df.withColumnRenamed("name","emp_name") #to change a column name
# df.withColumn("country",F.lit("India")) # to add a column with a constant value
# df.drop("department") #Drop column
# df.withColumn("salary",df.salary.cast("double")) # Changing datatype of a column

df = df.withColumnRenamed("name","employee_name")
df = df.withColumn("Bonus",F.lit(5000))
df = df.select("employee_id","employee_name","salary","bonus")
df.show(5)
