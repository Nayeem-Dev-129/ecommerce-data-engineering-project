# Databricks notebook source
#Base Path
BASE = "/Volumes/main/default/ecommerce_volume/bronze"

# COMMAND ----------

#Reading Parquet Files From Bronze Layer
orders = spark.read.parquet(f"{BASE}/orders")
order_products = spark.read.parquet(f"{BASE}/order_products")
products = spark.read.parquet(f"{BASE}/products")
departments = spark.read.parquet(f"{BASE}/departments")
aisles = spark.read.parquet(f"{BASE}/aisles")

# COMMAND ----------

#Cleaning  Orders Data
from pyspark.sql.functions import col, trim

orders_clean = orders.filter(
    trim(col("order_id")).rlike("^[0-9]+$")
).select(
    col("order_id").cast("int"),
    col("user_id").cast("int"),
    "eval_set",
    col("order_number").cast("int"),
    col("order_dow").cast("int"),
    col("order_hour_of_day").cast("int"),
    col("days_since_prior_order").cast("double")
)

# COMMAND ----------

#Cleaning  Orders_products Data
order_products_clean = order_products.filter(
    trim(col("order_id")).rlike("^[0-9]+$") &
    trim(col("product_id")).rlike("^[0-9]+$")
).select(
    col("order_id").cast("int"),
    col("product_id").cast("int"),
    col("add_to_cart_order").cast("int"),
    col("reordered").cast("int")
)

# COMMAND ----------

#Cleaning  products Data
products_clean = products.filter(
    trim(col("product_id")).rlike("^[0-9]+$") &
    trim(col("aisle_id")).rlike("^[0-9]+$") &
    trim(col("department_id")).rlike("^[0-9]+$")
).select(
    col("product_id").cast("int"),
    "product_name",
    col("aisle_id").cast("int"),
    col("department_id").cast("int")
)

# COMMAND ----------

#Cleaning  aisles Data
aisles_clean = aisles.filter(
    trim(col("aisle_id")).rlike("^[0-9]+$")
).select(
    col("aisle_id").cast("int"),
    "aisle"
)

# COMMAND ----------

#Cleaning  departments Data
departments_clean = departments.filter(
    trim(col("department_id")).rlike("^[0-9]+$")
).select(
    col("department_id").cast("int"),
    "department"
)

# COMMAND ----------

#Writing data into parquet again after cleaning and then reading it back
orders_clean.write.mode("overwrite").parquet(f"{BASE}/temp/orders_clean")
orders_clean = spark.read.parquet(f"{BASE}/temp/orders_clean")

order_products_clean.write.mode("overwrite").parquet(f"{BASE}/temp/order_products_clean")
order_products_clean = spark.read.parquet(f"{BASE}/temp/order_products_clean")

products_clean.write.mode("overwrite").parquet(f"{BASE}/temp/products_clean")
products_clean = spark.read.parquet(f"{BASE}/temp/products_clean")

aisles_clean.write.mode("overwrite").parquet(f"{BASE}/temp/aisles_clean")
aisles_clean = spark.read.parquet(f"{BASE}/temp/aisles_clean")

departments_clean.write.mode("overwrite").parquet(f"{BASE}/temp/departments_clean")
departments_clean = spark.read.parquet(f"{BASE}/temp/departments_clean")

# COMMAND ----------

#Joining the parquet files
silver_df = orders_clean \
    .join(order_products_clean, "order_id") \
    .join(products_clean, "product_id") \
    .join(aisles_clean, "aisle_id") \
    .join(departments_clean, "department_id")

# COMMAND ----------

#Writing the joined file into parquet
silver_df.write.mode("overwrite").parquet(f"{BASE}/silver/ecommerce_cleaned")