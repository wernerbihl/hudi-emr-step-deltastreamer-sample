# Hudi Deltastreamer with Pyspark + S3 + SQS and EMR

This project is a practical hands-on demonstration of how to use Hudi Deltastreamer for ingesting real time S3 delta data using Hudi Deltastreamer as proposed here: https://hudi.apache.org/blog/2021/08/23/s3-events-source/

## What we're building

![enter image description here](https://raw.githubusercontent.com/wernerbihl/hudi-emr-step-deltastreamer-sample/master/preview.png)

1. We'll create two S3 locations. The "In" location is where we can drop any files i.e. parquet/csv etc. The "Out" location is where the files will land after it's been transformed by a pyspark script running on the EMR cluster in real-time.
2. Then we'll set up a SQS queue listening on the S3 bucket for changes.
3. Then we'll add the script that'll do the transformation in Pyspark
4. Then we'll add a step to our EMR cluster that will receive events from the SQS events.

## Step 1: Create S3 Buckets

Create an S3 bucket with default settings and create 2 folders and note their S3 URI's for later. In my case I created:

```
s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/in/
s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/out/
```

## Step 2: Create SQS Queue

Go to SQS and create a standard SQS queue.
**Name**: Choose any name. I've named mine: "HudiSQS".

**Access Policy**: Click Advanced and then use the following JSON as your policy. Remember to change the Resource in the JSON below and lock it down based on your security requirements:

```json
{
  "Version": "2012-10-17",
  "Id": "example-ID",
  "Statement": [
    {
      "Sid": "example-statement-ID",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "SQS:*",
      "Resource": "arn:aws:sqs:eu-west-1:767220686680:*"
    }
  ]
}
```

After creation, take note of the SQS URL for Later.

## Step 3: Set up S3 SQS Events when objects get added

Go to the S3 bucket you created in step 1, and choose properties. Under "Event Notifications" click "Create event notification".

**Event Name**: Choose a sensible name. I named mine "Hudi SQS In Event"

**Prefix**: The relative path to your "In" S3 location. Based on the example in step 1, I made mine: "hudi_data/in/"

**Suffix**: I'm only interested in parquet files landing in my situation, so I made my suffix ".parquet"

**Event Types**: Check "All object create events"

**Destination**: SQS Queue

**Specify SQS queue**: Select the SQS queue you created in Step 2. If you get an error, please double check your Access Policy in Step 2 to make sure you have given the correct permissions.

I'd recommend you test the connection at this point by dropping a parquet file in the "In" location and then go to your SQS queue and click on "Send and receive messages" and seeing if you can see a message there.

## Step 4: Add some sample data to the "In" S3 location

There is a generator.py file in this repo that generates some sample data. You can run this script from anywhere that has access to the S3 buckets in step1. It uses pyspark to write 5 employee records using hudi. Obviously never use spark to write this little data, look at using Flink, boto3, pandas, polars etc. if you are writing anything that can fit in memory to write with a single server. This is purely a demonstration of how to write with hudi and pyspark.

Please make sure you have the pip dependencies installed: boto3, faker and pyspark.

Change the following three variables in the script depending on your setup:

```
database_name  =  "hudi" # This is the glue data catalog database
table_name  =  "hudi_in" # This is the glue data catalog table
in_path  =  "s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/in/"
```

With Amazon EMR release version 5.28.0 and later, Amazon EMR installs Hudi components by default when Spark, Hive, or Presto is installed. Otherwise you'll have to download hudi-spark-bundle from https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark-bundle and placing it somewhere accessible from your script.

Here's the script:

```python
# ----------------------------------------------------------------------------------------
# Generate some sample data
# -----------------------------------------------------------------------------------------

try:
  from pyspark.sql import SparkSession
  from faker import Faker

  print("All modules are loaded .....")

except Exception as e:
  print("Some modules are missing {} ".format(e))

fake = Faker()

# ----------------------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------------------

database_name = "hudi"
table_name = "hudi_in"
in_path = "s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/in/"

hudi_options = {
  'hoodie.database.name': database_name,
  'hoodie.table.name': table_name,
  'hoodie.datasource.hive_sync.database': database_name,
  'hoodie.datasource.hive_sync.table': table_name,
  'hoodie.datasource.hive_sync.create_managed_table': 'true',
  'hoodie.datasource.hive_sync.enable': 'true',
  'hoodie.datasource.hive_sync.mode': 'hms',
  'hoodie.datasource.write.precombine.field': 'ts',
  'hoodie.datasource.write.storage.type': 'COPY_ON_WRITE', # Or "MERGE_ON_READ"
  'hoodie.datasource.write.recordkey.field': 'emp_id',
  'hoodie.datasource.write.operation': 'upsert',
  'hoodie.datasource.write.reconcile.schema': 'true'
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
df.write.format("hudi").options(**hudi_options).mode("append").save(in_path)

df.show(vertical=True, truncate=False)
```

To run the script:

```
spark-submit --jars /usr/lib/hudi/hudi-spark-bundle.jar generator.py
```

## Step 5: Create our pyspark script

In this repo there's a very simple script called streamer.py. This file has lots of configuration, so please make sure you follow each line and make sure it's relevant to your setup.

This file will listen for the deltas streaming in from S3/SQS and do a basic transformation (mask the telephone number) before storing it in the "Out" S3 Location. Store the file somewhere on S3 and note it's location for the next step.

```python
# streamer.py
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

sqs_url = 'https://sqs.eu-west-1.amazonaws.com/123456789012/HudiSQS'
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
```

## Step 6: Add a step to EMR cluster

We need a script to be continuously running on the EMR cluster to listen for SQS events and stream the results to a pyspark script for transformation.

Create an EMR cluster with Spark. You need to have a jar available on the cluster. Download:
https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-sqs
And place it somewhere on the EMR master or on S3. I've placed mine at: /home/hadoop/aws-java-sdk-sqs-1.12.390.jar

Then add a step:

**Step Type**: "Custom JAR"

**Name**: Can be anything. I named mine "hudi_deltastreamer"

**JAR location**: command-runner.jar

**Arguments**:
spark-submit --jars /home/hadoop/aws-java-sdk-sqs-1.12.390.jar,/usr/lib/hudi/hudi-spark-bundle.jar --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/streamer.py --continuous --enable-sync --master yarn --deploy-mode client --source-class org.apache.hudi.utilities.sources.S3EventsSource

Please review each argument and make sure you understand each one before adding the step and adapt it to your needs.

## Done

You can now keep using the generate.py file to push new data to the "In" location, and seconds later it should be transformed and in the "Out" location

## Extra

There is a basics.py file in the repo that shows how to use Hudi to do create, append, update, delete, cluster cleanup, time travel, incremental queries, schema evolution and how to use dynamo as a lock table.
