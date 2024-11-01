#!/bin/bash

#source "$HOME/.cargo/env"

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab --ip=0.0.0.0'
export DELTA_SPARK_VERSION='3.1.0'
export DELTA_PACKAGE_VERSION=delta-spark_2.12:${DELTA_SPARK_VERSION}


$SPARK_HOME/sbin/start-thriftserver.sh \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.warehouse.dir=/opt/spark/delta-tables \
  --conf spark.history.fs.logDirectory=/opt/spark/thrift-logs \
  --packages 'io.delta:delta-core_2.12:2.3.0'