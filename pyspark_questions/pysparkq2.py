"""
𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
Given two datasets, products and sales, write a PySpark program to identify products that have never been sold.
Assume the schema of products is (product_id, product_name) and the schema of sales is (sale_id, product_id, sale_date).
𝐬𝐜𝐡𝐞𝐦𝐚
# Create DataFrame for products

products = spark.createDataFrame([ (1, "Laptop"), (2, "Tablet"),
(3, "Smartphone"), (4, "Monitor"),
(5, "Keyboard") ], ["product_id", "product_name"])

# Create DataFrame for sales

sales = spark.createDataFrame([ (101, 1, "2025-01-01"), (102, 3, "2025-01-02"), (103, 5, "2025-01-03") ], ["sale_id", "product_id", "sale_date"])

"""

from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

products = spark.createDataFrame([ (1, "Laptop"), (2, "Tablet"),
(3, "Smartphone"), (4, "Monitor"),
(5, "Keyboard") ], ["product_id", "product_name"])

sales = spark.createDataFrame([ (101, 1, "2025-01-01"), (102, 3, "2025-01-02"), (103, 5, "2025-01-03") ], ["sale_id", "product_id", "sale_date"])

products.show()
sales.show()

unsold_products=products.join(sales,on="product_id",how="left_anti")
unsold_products.show()

#
# """
# Write a PySpark query to find students who are not enrolled in any course.
# students = spark.createDataFrame([
#     (1, "Alice"),
#     (2, "Bob"),
#     (3, "Charlie"),
#     (4, "Diana")
# ], ["student_id", "student_name"])

# enrollments = spark.createDataFrame([
#     (1, "Math"),
#     (2, "Science"),
#     (2, "English"),
#     (3, "Math")
# ], ["student_id", "course"])
# """

students=spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "Diana")
], ["student_id", "student_name"])


enrollments = spark.createDataFrame([
    (1, "Math"),
    (2, "Science"),
    (2, "English"),
    (3, "Math")
], ["student_id", "course"])

without_enrollments=students.join(enrollments,on="student_id",how="left_anti")
without_enrollments.show()






