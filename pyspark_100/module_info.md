Module 1 — Fundamentals
- [ ] 1. Create a DataFrame: Create a DataFrame from a list of tuples.
- [ ] 2. Display records: Show the first 5 records.
- [ ] 3. Inspect schema: Print column names and data types.
- [ ] 4. Count records: Find the total number of rows.
- [ ] 5. Count columns: Find the number of columns.
- [ ] 6. Rename a column: Rename `name` to `emp_name`.
- [ ] 7. Add a column: Add a column `country` with value India.
- [ ] 8. Drop a column: Remove the `city` column.
- [ ] 9. Change data type: Convert salary from string to integer.
- [ ] 10. Select columns: Display only employee name and salary.

Module 2 — Filtering and sorting
- [ ] 11. Filter salary: Find employees earning more than 60000.
- [ ] 12. Multiple conditions: Find IT employees earning above 50000.
- [ ] 13. OR condition: Find employees in HR or Finance.
- [ ] 14. Filter cities: Find employees living in Pune or Mumbai.
- [ ] 15. Exclude a value: Find employees not in the IT department.
- [ ] 16. Sort ascending: Sort employees by salary, lowest first.
- [ ] 17. Sort descending: Sort employees by salary, highest first.
- [ ] 18. Distinct values: List unique department names.
- [ ] 19. Distinct records: Remove completely duplicate rows.
- [ ] 20. Top records: Find the 3 highest-paid employees.

Module 3 — Aggregations
- [ ] 21. Count by department: Count employees in each department.
- [ ] 22. Average salary: Calculate the average salary per department.
- [ ] 23. Maximum salary: Find the highest salary per department.
- [ ] 24. Minimum salary: Find the lowest salary per department.
- [ ] 25. Total payroll: Calculate total salary per department.
- [ ] 26. Multiple aggregates: Find count, average, minimum and maximum salary by department.
- [ ] 27. Conditional count: Count employees earning above 60000 per department.
- [ ] 28. Filter groups: Find departments with more than 2 employees.
- [ ] 29. Filter average: Find departments whose average salary exceeds 55000.
- [ ] 30. Salary range: Calculate max salary minus min salary per department.

Module 4 — Joins
- [ ] 31. Inner join: Join employee and department DataFrames.
- [ ] 32. Left join: Keep all employees even if a department match is missing.
- [ ] 33. Right join: Keep all department records.
- [ ] 34. Full outer join: Return matched and unmatched records from both sides.
- [ ] 35. Left anti join: Find employees without a matching department.
- [ ] 36. Left semi join: Find employees with a matching department.
- [ ] 37. Join on multiple keys: Join using employee ID and department.
- [ ] 38. Find unmatched records: Identify source records missing from the target.
- [ ] 39. Avoid duplicate columns: Join two DataFrames with overlapping column names.
- [ ] 40. Join and aggregate: Calculate total orders per customer after joining customers and orders.

Module 5 — Conditions, strings, dates and nulls
- [ ] 41. Salary category: Label salaries as High, Medium or Low using `when`.
- [ ] 42. Multiple conditions: Classify employees based on department and salary.
- [ ] 43. Handle nulls: Replace missing values using `fillna`.
- [ ] 44. Filter nulls: Find records with a missing salary.
- [ ] 45. Coalesce values: Use a backup email when the primary email is null.
- [ ] 46. String cleanup: Trim spaces and standardize employee names.
- [ ] 47. Substring: Extract a portion of a string column.
- [ ] 48. Date difference: Calculate days between two dates.
- [ ] 49. Extract date parts: Extract year and month from a date.
- [ ] 50. Conditional aggregation: Calculate the count of high-salary employees per department.

Module 6 — Window functions
- [ ] 51. Row numbering: Number employees by salary within each department.
- [ ] 52. Rank salaries: Rank employees by salary within each department.
- [ ] 53. Dense ranking: Assign ranks without gaps when salaries tie.
- [ ] 54. Highest salary: Find the highest-paid employee in each department.
- [ ] 55. Second-highest salary: Find the second-highest distinct salary per department.
- [ ] 56. Top 2 employees: Return the top 2 employees per department.
- [ ] 57. Nth-highest salary: Find the third-highest distinct salary per department.
- [ ] 58. Previous salary: Use `lag` to retrieve the previous row's salary.
- [ ] 59. Next salary: Use `lead` to retrieve the next row's salary.
- [ ] 60. Salary comparison: Find employees earning more than the previous row.
- [ ] 61. Department average: Show each employee alongside the department average.
- [ ] 62. Difference from average: Calculate salary minus department average.
- [ ] 63. Running total: Calculate cumulative salary by department.
- [ ] 64. First and last value: Find the first and last salary in each department.
- [ ] 65. Salary changes: Compare each employee's monthly salary with their previous month's salary.

Module 7 — Deduplication and data quality
- [ ] 66. Find duplicate keys: Identify employee IDs appearing more than once.
- [ ] 67. Keep latest record: Keep the latest row per ID using `row_number`.
- [ ] 68. Duplicate salary groups: Find employees sharing the same salary within a department.
- [ ] 69. Null quality report: Count nulls in important columns.
- [ ] 70. Validate ranges: Find negative or implausible salary values.
- [ ] 71. Detect duplicate events: Identify duplicate event IDs.
- [ ] 72. Deduplicate by business key: Keep one record per customer using update timestamp.
- [ ] 73. Conflicting duplicates: Find IDs whose duplicate rows contain different values.
- [ ] 74. Required-field validation: Separate valid rows from rows missing required fields.
- [ ] 75. Data quality summary: Produce row count, duplicate count and null count.

Module 8 — ETL, CDC and SCD
- [ ] 76. Source-target comparison: Find source records missing from the target.
- [ ] 77. Find new records: Identify source keys that don't exist in the target.
- [ ] 78. Find deleted records: Identify target keys missing from a complete source snapshot.
- [ ] 79. Find updated records: Detect matching keys whose attributes changed.
- [ ] 80. Compare multiple columns: Detect changes across several attributes.
- [ ] 81. Build an incremental load: Filter records using a last-updated timestamp.
- [ ] 82. SCD Type 1: Overwrite changed attributes while retaining new records.
- [ ] 83. SCD Type 2: Expire the old version and insert a new version.
- [ ] 84. Idempotent processing: Design a rerunnable load without duplicate inserts.
- [ ] 85. CDC event ordering: Handle multiple changes for one key and select the latest valid event.

Module 9 — Reading and writing data
- [ ] 86. Read CSV: Read CSV with headers and an explicit schema.
- [ ] 87. Read JSON: Load nested JSON records.
- [ ] 88. Write Parquet: Write a DataFrame to Parquet.
- [ ] 89. Write modes: Explain and demonstrate append, overwrite, error and ignore.
- [ ] 90. Partitioned output: Write data partitioned by department or date.

Module 10 — Performance
- [ ] 91. Repartition: Change the number of partitions using `repartition`.
- [ ] 92. Coalesce: Reduce partitions using `coalesce`.
- [ ] 93. Broadcast join: Join a large DataFrame with a small lookup DataFrame.
- [ ] 94. Cache data: Cache a DataFrame reused by multiple actions.
- [ ] 95. Avoid unnecessary shuffle: Compare `reduceByKey`-style thinking with grouping/shuffling concepts.
- [ ] 96. Data skew: Identify and explain a key causing uneven task durations.
- [ ] 97. Explain execution plan: Use `explain()` to inspect a query plan.
- [ ] 98. Optimize file sizes: Explain why thousands of tiny output files can be problematic.
- [ ] 99. Reduce data early: Push filters and select only needed columns before a join.
- [ ] 100. Debug slow jobs: Investigate slow stages, shuffles and uneven partitions.