# ----------------------------------------------------------------------------------------
# Generate some sample data
# -----------------------------------------------------------------------------------------


try:
  import os
  import sys
  import uuid
  import boto3

  from pyspark.sql import SparkSession
  from pyspark.sql.functions import col, asc, desc
  from faker import Faker

  print("All modules are loaded .....")

except Exception as e:
  print("Some modules are missing {} ".format(e))

fake = Faker()

# ----------------------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------------------

database_name = "hudi_raw"
table_name = "hudi_raw"
raw_path = "s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/raw/"

hudi_options = {
  'hoodie.table.name': table_name,
  "hoodie.datasource.write.storage.type": "COPY_ON_WRITE", # Or "MERGE_ON_READ"
  'hoodie.datasource.write.recordkey.field': 'emp_id',
  'hoodie.datasource.write.table.name': table_name,
  'hoodie.datasource.write.operation': 'upsert',
  'hoodie.datasource.write.precombine.field': 'ts',
  'hoodie.datasource.hive_sync.enable': 'true',
  "hoodie.datasource.hive_sync.mode":"hms",
  'hoodie.datasource.hive_sync.sync_as_datasource': 'false',
  'hoodie.datasource.hive_sync.database': database_name,
  'hoodie.datasource.hive_sync.table': table_name,
  'hoodie.datasource.hive_sync.use_jdbc': 'false',
  'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
  'hoodie.datasource.write.hive_style_partitioning': 'true',
}

# ----------------------------------------------------------------------------------------
# Spark Initialization
# -----------------------------------------------------------------------------------------

spark = SparkSession \
  .builder \
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
  .config("spark.sql.hive.convertMetastoreParquet", "false") \
  .getOrCreate()

# ----------------------------------------------------------------------------------------
# Create Sample Data
# -----------------------------------------------------------------------------------------

columns = ["emp_id", "employee_name", "phone", "department", "state", "salary", "age", "bonus", "ts"]

def get_data(amount):
  return [
    (
      fake.random_int(min=1, max=9999999999),
      fake.name(),
      fake.phone_number(),
      fake.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
      fake.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
      fake.random_int(min=10000, max=150000),
      fake.random_int(min=18, max=60),
      fake.random_int(min=0, max=100000),
      fake.unix_time()
    ) for x in range(amount)
  ]

data = get_data(5)
df = spark.createDataFrame(data=data, schema=columns)
df.write.format("hudi").options(**hudi_options).mode("append").save(raw_path)

df.show(vertical=True, truncate=False)