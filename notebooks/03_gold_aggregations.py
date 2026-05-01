# Databricks notebook source
#Base Path
BASE = "/Volumes/main/default/ecommerce_volume"
silver_df = spark.read.parquet(f"{BASE}/silver/ecommerce_cleaned")

# COMMAND ----------

#Finding Top Products
from pyspark.sql.functions import *
top_products = silver_df.groupBy("product_name")\
                .count()\
                .withColumnRenamed('count','top_products')\
                .orderBy(col('top_products').desc())
top_products.limit(10).display()
top_products.write.mode("overwrite").parquet(f"{BASE}/gold/top_products")

# COMMAND ----------

#Finding Departments Sales
department_sales = silver_df.groupBy("department") \
                    .count() \
                    .withColumnRenamed("count", "total_orders") \
                    .orderBy(col("total_orders").desc())
department_sales.limit(10).display()
department_sales.write.mode("overwrite").parquet(f"{BASE}/gold/department_sales")

# COMMAND ----------

#FInding Aisle Sales
aisle_sales = silver_df.groupBy("aisle") \
                .count() \
                .withColumnRenamed("count", "total_orders") \
                .orderBy(col("total_orders").desc())
aisle_sales.limit(10).display()
aisle_sales.write.mode("overwrite").parquet(f"{BASE}/gold/aisle_sales")

# COMMAND ----------

#Finding Reordered Rate
from pyspark.sql.functions import avg

reorder_rate = silver_df.select(
    avg(col("reordered")).alias("reorder_rate")
)
reorder_rate.display()
reorder_rate.write.mode("overwrite").parquet(f"{BASE}/gold/reorder_rate")

# COMMAND ----------

#Finding Orders per day
orders_by_day = silver_df.groupBy("order_dow") \
                .count() \
                .withColumnRenamed("count", "total_orders") \
                .orderBy(col("total_orders").desc())
from pyspark.sql.functions import when, col

orders_by_day_named = orders_by_day.withColumn(
    "day_name",
    when(col("order_dow") == 0, "Sunday")
    .when(col("order_dow") == 1, "Monday")
    .when(col("order_dow") == 2, "Tuesday")
    .when(col("order_dow") == 3, "Wednesday")
    .when(col("order_dow") == 4, "Thursday")
    .when(col("order_dow") == 5, "Friday")
    .when(col("order_dow") == 6, "Saturday")
)
orders_by_day_named.limit(10).display()

orders_by_day_named.write.mode("overwrite").parquet(f"{BASE}/gold/orders_by_day")

# COMMAND ----------

#Finding Orders per hour
orders_by_hour = silver_df.groupBy("order_hour_of_day") \
                .count() \
                .withColumnRenamed("count", "total_orders") \
                .orderBy(col("total_orders").desc())
from pyspark.sql.functions import col, when

orders_by_hour_named = orders_by_hour.withColumn(
    "hour_label",
    when(col("order_hour_of_day") == 0, "12 AM")
    .when(col("order_hour_of_day") == 1, "1 AM")
    .when(col("order_hour_of_day") == 2, "2 AM")
    .when(col("order_hour_of_day") == 3, "3 AM")
    .when(col("order_hour_of_day") == 4, "4 AM")
    .when(col("order_hour_of_day") == 5, "5 AM")
    .when(col("order_hour_of_day") == 6, "6 AM")
    .when(col("order_hour_of_day") == 7, "7 AM")
    .when(col("order_hour_of_day") == 8, "8 AM")
    .when(col("order_hour_of_day") == 9, "9 AM")
    .when(col("order_hour_of_day") == 10, "10 AM")
    .when(col("order_hour_of_day") == 11, "11 AM")
    .when(col("order_hour_of_day") == 12, "12 PM")
    .when(col("order_hour_of_day") == 13, "1 PM")
    .when(col("order_hour_of_day") == 14, "2 PM")
    .when(col("order_hour_of_day") == 15, "3 PM")
    .when(col("order_hour_of_day") == 16, "4 PM")
    .when(col("order_hour_of_day") == 17, "5 PM")
    .when(col("order_hour_of_day") == 18, "6 PM")
    .when(col("order_hour_of_day") == 19, "7 PM")
    .when(col("order_hour_of_day") == 20, "8 PM")
    .when(col("order_hour_of_day") == 21, "9 PM")
    .when(col("order_hour_of_day") == 22, "10 PM")
    .when(col("order_hour_of_day") == 23, "11 PM")
)
orders_by_hour_named.limit(10).display()
orders_by_hour_named.write.mode("overwrite").parquet(f"{BASE}/gold/orders_by_hour")

# COMMAND ----------

#Finding Top Reordered Products
top_reordered = silver_df.filter(col("reordered") == 1) \
                .groupBy("product_name") \
                .count() \
                .withColumnRenamed("count", "reorder_count") \
                .orderBy(col("reorder_count").desc())
top_reordered.limit(10).display()
top_reordered.write.mode("overwrite").parquet(f"{BASE}/gold/top_reordered_products")

# COMMAND ----------

#Finding Top First Time Ordered Products
top_first_time = silver_df.filter(col("reordered") == 0) \
    .groupBy("product_name") \
    .count() \
    .withColumnRenamed("count", "ordered_first_time") \
    .orderBy(col("ordered_first_time").desc())
top_first_time.limit(10).display()
top_first_time.write.mode("overwrite").parquet(f"{BASE}/gold/top_first_time_products")

# COMMAND ----------

