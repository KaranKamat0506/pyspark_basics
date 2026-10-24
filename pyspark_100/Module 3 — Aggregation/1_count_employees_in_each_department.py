from pyspark.sql import SparkSession

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

columns = ["employee_id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)

result_df = df.groupBy("department").count().show()
# groupBy("department") → Groups employees based on their department.
# count() → Counts how many employees are present in each group.
# show() → Displays the result.

# Interview memory trick:
# groupBy() = Group the data
# count() = Count the records
# sum() = Add the values
# avg() = Calculate the average
