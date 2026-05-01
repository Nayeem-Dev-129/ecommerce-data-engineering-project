# Databricks notebook source
#Base path for the csv files in the volume created
BASE = "/Volumes/main/default/ecommerce_volume"

# COMMAND ----------

#Reading csv files from the volume
orders = spark.read.csv(f"{BASE}/orders.csv", header=True, inferSchema=True)
order_products = spark.read.csv(f"{BASE}/order_products__prior.csv", header=True, inferSchema=True)
products = spark.read.csv(f"{BASE}/products.csv", header=True, inferSchema=True)
aisles = spark.read.csv(f"{BASE}/aisles.csv", header=True, inferSchema=True)
departments = spark.read.csv(f"{BASE}/departments.csv", header=True, inferSchema=True)

# COMMAND ----------

#Writing the csv files into the parquet form
orders.write.mode("overwrite").parquet(f"{BASE}/bronze/orders")
order_products.write.mode("overwrite").parquet(f"{BASE}/bronze/order_products")
products.write.mode("overwrite").parquet(f"{BASE}/bronze/products")
aisles.write.mode("overwrite").parquet(f"{BASE}/bronze/aisles")
departments.write.mode("overwrite").parquet(f"{BASE}/bronze/departments")

# COMMAND ----------

#Verifying the files being written
display(dbutils.fs.ls(f"{BASE}/bronze/"))

# COMMAND ----------

