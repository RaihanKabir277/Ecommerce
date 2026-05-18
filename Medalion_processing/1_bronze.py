# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

brand_schema = StructType([
  StructField('brand_code', StringType(), False),
  StructField('brand_name', StringType(), True),
  StructField('category_code', StringType(), True),
])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC read_files(
# MAGIC   '/Volumes/ecommerce/source_data/raw/brands/brands.csv',
# MAGIC   format => 'csv'
# MAGIC )

# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/brands.csv"
df = spark.read.option('header', 'True').option('delimiter', ',').schema(brand_schema).csv(raw_data_path)

df = df.withColumn("source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df)

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.bronz_brands")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce.bronze.bronz_brands

# COMMAND ----------

# MAGIC %md
# MAGIC CATAGORY

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC read_files(
# MAGIC   '/Volumes/ecommerce/source_data/raw/category/category.csv',
# MAGIC   format => 'csv'
# MAGIC )

# COMMAND ----------

category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])

# Load data
raw_data_path = "/Volumes/ecommerce/source_data/raw/category/category.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(category_schema).csv(raw_data_path)

df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.col("_metadata.file_path"))


# Write raw data to the Bronze layer
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.bronz_category") 

# COMMAND ----------

# MAGIC %md
# MAGIC customers
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC read_files(
# MAGIC   '/Volumes/ecommerce/source_data/raw/customers/customers.csv',
# MAGIC   format => 'csv'
# MAGIC )

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

# Load data using the schema defined
raw_data_path ="/Volumes/ecommerce/source_data/raw/customers/customers.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(customers_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp())

# Write raw data to the Bronze layer 
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.bronz_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC products
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC read_files(
# MAGIC   '/Volumes/ecommerce/source_data/raw/products/products.csv',
# MAGIC   format => 'csv'
# MAGIC )

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True), 
    StructField("length_cm", StringType(), True),  
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("file_name", StringType(), False),
    StructField("ingest_timestamp", TimestampType(), False)
])
# Load data
raw_data_path = "/Volumes/ecommerce/source_data/raw/products/products.csv"

df = spark.read.option("header", "true").option("delimiter", ",").schema(products_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp())

# Write raw data to the Bronze layer 
df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_products")   

# COMMAND ----------

# MAGIC %md
# MAGIC date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC read_files(
# MAGIC   '/Volumes/ecommerce/source_data/raw/date/date.csv',
# MAGIC   format => 'csv'
# MAGIC )

# COMMAND ----------

date_schema = StructType([
    StructField("date", StringType(), True),         
    StructField("year", IntegerType(), True),         
    StructField("day_name", StringType(), True),     
    StructField("quarter", IntegerType(), True),       
    StructField("week_of_year", IntegerType(), True), 
])

# Load data
raw_data_path = f"/Volumes/ecommerce/source_data/raw/date/date.csv" 

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(date_schema).csv(raw_data_path)

df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.col("_metadata.file_path"))


# Write raw data to the Bronze layer 
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.bronz_calendar")  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC read_files(
# MAGIC   '/Volumes/ecommerce/source_data/raw/order_items/landing/',
# MAGIC   format => 'csv'
# MAGIC )