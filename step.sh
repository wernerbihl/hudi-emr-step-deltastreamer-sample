spark-submit \
  --jars "/home/hadoop/aws-java-sdk-sqs-1.12.390.jar,/usr/lib/hudi/hudi-spark-bundle.jar" \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/streamer.py \
  --continuous \
  --enable-sync \
  --master yarn \
  --deploy-mode client \
  --target-table hudi_processed \
  --table-type COPY_ON_WRITE \
  --source-ordering-field ts \
  --target-base-path s3://oml-dp-dataplatform-datalabs-eu-west-1/hudi_data/raw/ \
  --source-class org.apache.hudi.utilities.sources.S3EventsSource