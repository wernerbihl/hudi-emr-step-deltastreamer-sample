# ----------------------------------------------------------------------------------------
# Hudi Basics
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

faker = Faker()

# ----------------------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------------------

database_name = "hudi_raw"
table_name = "hudi_raw"
raw_path = "s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/raw/"

curr_session = boto3.session.Session()
curr_region = curr_session.region_name

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

  # (Optional) These settings are for configuting DynamoDB Lock table
  'hoodie.write.concurrency.mode' : 'optimistic_concurrency_control',
  'hoodie.cleaner.policy.failed.writes' : 'LAZY',
  'hoodie.write.lock.provider' : 'org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider',
  'hoodie.write.lock.dynamodb.table' : 'hudi-lock-table',
  'hoodie.write.lock.dynamodb.partition_key' : 'tablename',
  'hoodie.write.lock.dynamodb.region' : '{0}'.format(curr_region),
  'hoodie.write.lock.dynamodb.endpoint_url' : 'dynamodb.{0}.amazonaws.com'.format(curr_region),
  'hoodie.write.lock.dynamodb.billing_mode' : 'PAY_PER_REQUEST',
  'hoodie.bulkinsert.shuffle.parallelism': 2000
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
      x,
      faker.name(),
      faker.phone_number(),
      faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
      faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
      faker.random_int(min=10000, max=150000),
      faker.random_int(min=18, max=60),
      faker.random_int(min=0, max=100000),
      faker.unix_time()
    ) for x in range(amount)
  ]

data = get_data(5)
df = spark.createDataFrame(data=data, schema=columns)
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(raw_path)

# ----------------------------------------------------------------------------------------
# Append Data
# -----------------------------------------------------------------------------------------

appendData = [
  (6, "This is APPEND", "+27723123111" "Sales", "RJ", 81000, 30, 23000, 827307999),
  (7, "This is APPEND", "+41723123122" "Engineering", "RJ", 79000, 53, 15000, 1627694678),
]

df = spark.createDataFrame(data=appendData, schema=columns)
df.write.format("hudi").options(**hudi_options).mode("append").save(raw_path)

# ----------------------------------------------------------------------------------------
# Update Data
# -----------------------------------------------------------------------------------------

updateData = [
  (3, "this is update on data lake", "+27723123333", "Sales", "RJ", 81000, 30, 23000, 827307999),
]

df = spark.createDataFrame(data=updateData, schema=columns)
df.write.format("hudi").options(**hudi_options).mode("append").save(raw_path)

# ----------------------------------------------------------------------------------------
# Delete Data
# -----------------------------------------------------------------------------------------

df = spark.sql("SELECT * FROM hudi_raw.hudi_raw where emp_id=4 ")

hudi_options['hoodie.datasource.write.operation'] = 'delete'
df.write.format("hudi").options(**hudi_options).mode("append").save(raw_path)

# ----------------------------------------------------------------------------------------
# Compacting
# -----------------------------------------------------------------------------------------

hudi_options['hoodie.clustering.plan.strategy.sort.columns'] = 'department'
hudi_options['hoodie.clustering.plan.strategy.max.bytes.per.group'] = '107374182400'
hudi_options['hoodie.clustering.plan.strategy.max.num.groups'] = '1'
hudi_options['hoodie.cleaner.policy'] = 'KEEP_LATEST_FILE_VERSIONS'

df = spark.sql("SELECT * FROM hudidb.hudi_table ")
df.write.format("hudi").options(**hudi_options).mode("append").save(raw_path)

# ----------------------------------------------------------------------------------------
# Time Travel Query
# -----------------------------------------------------------------------------------------

df = spark.read.format("hudi").option("as.of.instant", "2022-01-01 00:00:00").load(raw_path)

# ----------------------------------------------------------------------------------------
# Incremental query
# -----------------------------------------------------------------------------------------

df = spark.read.format("hudi").option("hoodie.datasource.query.type", "incremental").load(raw_path)

# ----------------------------------------------------------------------------------------
# Schema evolution
# -----------------------------------------------------------------------------------------

df = spark.read.format("hudi").option("mergeSchema", "true").load(raw_path)
