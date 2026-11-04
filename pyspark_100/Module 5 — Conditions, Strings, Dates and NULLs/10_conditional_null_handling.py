#10_10_conditional_null_handling
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

null_data = [
    (1, "Rahul", "IT", 50000),
    (2, "Priya", None, 65000),
    (3, "Amit", "IT", None),
    (4, "Sneha", "Finance", 55000),
    (5, "Neha", None, 80000)
]

null_columns = ["employee_id", "name", "department", "salary"]

null_df = spark.createDataFrame(null_data, null_columns)

result_df = null_df.withColumn(
    "department_status",
    F.when(
        F.col("department").isNull(),
        "Not Assigned"
    ).otherwise("Available")
)
result_df.show()

"""
Practice Problem 50
Using the same null_df dataset:
Requirements:
1. Create a new column called clean_department.
   - If department is NULL, assign "Not Assigned".
   - Otherwise, retain the original department.
2. Create another column called clean_salary.
   - If salary is NULL, assign 0.
   - Otherwise, retain the original salary.
3. Create a third column called salary_status.
   - If salary is NULL, assign "Missing".
   - Otherwise, assign "Available".
4. Display the following columns:
   - name
   - department
   - clean_department
   - salary
   - clean_salary
   - salary_status
"""

result_df_2=null_df.withColumn(
    "clean_department",
    F.when(
        F.col("department").isNull(),
        "Not Assigned"
    ).otherwise(F.col("department"))
).withColumn(
    "clean_salary",
    F.when(
        F.col("salary").isNull(),
        0
    ).otherwise(F.col("salary"))
).withColumn(
    "salary_status",
    F.when(
        F.col("salary").isNull(),
        "Missing"
    ).otherwise("Available")
)

result_df_2.select("name","department","salary","clean_salary","salary_status").show()
