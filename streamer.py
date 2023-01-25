from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, asc, desc

spark = SparkSession \
  .builder \
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
  .config("spark.sql.hive.convertMetastoreParquet", "false") \
  .getOrCreate()

hive_database = "hudi_raw"
hive_target_table = "hudi_processed"

checkpoint_location = "file:///tmp/checkpoints/hudi_raw"
raw_path = "s3a://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/raw/"
processed_path = "s3a://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/processed/"
partition_by = 'department'

hudi_streaming_options = {
  'hoodie.table.name': hive_target_table,
  'hoodie.deltastreamer.s3.source.queue.url': 'https://sqs.eu-west-1.amazonaws.com/767220686680/HudiSQS',
  'hoodie.deltastreamer.s3.source.queue.region': 'eu-west-1',
  'hoodie.datasource.write.recordkey.field': 'emp_id',
  'hoodie.datasource.write.partitionpath.field': partition_by,
  'hoodie.datasource.write.table.name': hive_target_table,
  'hoodie.datasource.write.operation': 'upsert',
  'hoodie.datasource.write.precombine.field': partition_by,
  'hoodie.datasource.write.hive_style_partitioning': True,
  'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.SimpleKeyGenerator',
  'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
  'hoodie.datasource.hive_sync.database': hive_database,
  'hoodie.datasource.hive_sync.partition_fields': partition_by,
  'hoodie.datasource.hive_sync.table': hive_target_table,
  'hoodie.upsert.shuffle.parallelism': 2,
  'hoodie.insert.shuffle.parallelism': 2,
}

df = spark.readStream.format("hudi").load(raw_path)

# Do basic transformations here
df = df.withColumn("phone", lit("*** Masked ***"))

# write stream to new hudi table
df.writeStream.format("hudi") \
    .options(**hudi_streaming_options) \
    .outputMode("append") \
    .option("path", processed_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start() \
    .awaitTermination()