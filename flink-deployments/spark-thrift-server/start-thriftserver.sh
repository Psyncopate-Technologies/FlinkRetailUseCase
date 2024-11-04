#!/bin/bash

# Set any necessary environment variables
export DELTA_SPARK_VERSION='3.1.0'

# Start the Spark Thrift Server with Delta Lake configurations
$SPARK_HOME/sbin/start-thriftserver.sh \
  --master local[*] \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.warehouse.dir=/opt/spark/delta-tables \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --packages io.delta:delta-core_2.12:$DELTA_SPARK_VERSION
