# ----------------------------------------------------------------------------------------
# Script for streaming data from Hudi Deltastreamer
# -----------------------------------------------------------------------------------------

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession \
  .builder \
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
  .config("spark.sql.hive.convertMetastoreParquet", "false") \
  .getOrCreate()

# ----------------------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------------------

curr_session = boto3.session.Session()
curr_region = curr_session.region_name

sqs_url = 'https://sqs.eu-west-1.amazonaws.com/767220686680/HudiSQS'
database_name = "hudi"
table_name = "hudi_out"
checkpoint_location = 's3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/checkpoints/'
in_path = "s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/in/"
out_path = "s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/out/"
partition_by = 'department' # (Optional) Partitions your data based on a field

hudi_streaming_options = {
  'hoodie.table.name': table_name,
  'hoodie.database.name': database_name,
  'hoodie.deltastreamer.s3.source.queue.url': sqs_url,
  'hoodie.deltastreamer.s3.source.queue.region': curr_region,
  'hoodie.datasource.hive_sync.database': database_name,
  'hoodie.datasource.hive_sync.table': table_name,
  'hoodie.datasource.hive_sync.create_managed_table': 'true',
  'hoodie.datasource.hive_sync.enable': 'true',
  'hoodie.datasource.hive_sync.mode': 'hms',
  "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
  'hoodie.datasource.write.recordkey.field': 'emp_id',
  'hoodie.datasource.write.partitionpath.field': partition_by,
  'hoodie.datasource.write.operation': 'upsert',
  'hoodie.datasource.write.precombine.field': 'ts',
  'hoodie.datasource.write.hive_style_partitioning': 'true',
  'hoodie.datasource.write.reconcile.schema': 'true'
}

# ----------------------------------------------------------------------------------------
# Read stream and do transformations
# -----------------------------------------------------------------------------------------

df = spark.readStream.format("hudi").load(in_path)

df = df.withColumn("phone", lit("*** Masked ***"))

df.writeStream.format("hudi") \
    .options(**hudi_streaming_options) \
    .outputMode("append") \
    .option("path", out_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start() \
    .awaitTermination()