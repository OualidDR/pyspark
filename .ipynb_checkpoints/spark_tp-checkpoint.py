from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
    .master("local[4]")                            # 1 machine, 4 cores = 4 task slots
    .appName("tp")
    .config("spark.sql.shuffle.partitions", "8")   # 200 is too much for a laptop
    .getOrCreate())

# Customers: 1000 rows
customers = spark.range(1, 1001).select(
    F.col("id").alias("cust_id"),
    F.concat(F.lit("name_"), F.col("id")).alias("name"),
    F.element_at(F.array(F.lit("MA"), F.lit("FR"), F.lit("US"), F.lit("DE")),
                 (F.col("id") % 4 + 1).cast("int")).alias("country"))

# Orders: 1,000,000 rows, and half of them belong to customer 1 (a hot key, for the skew lab)
orders = spark.range(1_000_000).select(
    F.col("id").alias("order_id"),
    F.when(F.rand(42) < 0.5, F.lit(1))
     .otherwise((F.rand(7) * 1000).cast("int") + 1).alias("cust_id"),
    (F.rand(1) * 500).alias("amount"))

# Employees: 5000 rows, 10 departments
employees = spark.range(5000).select(
    F.col("id").alias("emp_id"),
    F.concat(F.lit("emp_"), F.col("id")).alias("name"),
    (F.col("id") % 10).alias("dept_id"),
    (F.rand(3) * 8000 + 2000).cast("int").alias("salary"),
    F.date_add(F.lit("2015-01-01"), (F.rand(5) * 3000).cast("int")).alias("hire_date"))

# Save as Parquet so every lab reads the same data
customers.write.mode("overwrite").parquet("data/customers")
orders.write.mode("overwrite").parquet("data/orders")
employees.write.mode("overwrite").parquet("data/employees")
