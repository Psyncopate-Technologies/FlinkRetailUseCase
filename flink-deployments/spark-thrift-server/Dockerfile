FROM eclipse-temurin:11-jre-alpine


WORKDIR /spark

ENV SPARK_VERSION 3.2.1
ENV HADOOP_VERSION 3.2
ENV DELTA_VERSION 1.1.0

ENV SPARK_ARCHIVE spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN wget "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}"
RUN tar -xf $SPARK_ARCHIVE
RUN rm $SPARK_ARCHIVE

ENV SPARK_DIR "/spark/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"

# Download Delta Lake JAR
RUN wget "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar" -P ${SPARK_DIR}/jars

EXPOSE 10000

ENTRYPOINT java \
 -Duser.timezone=Etc/UTC \
 -Xmx512m \
 -cp "${SPARK_DIR}/conf:${SPARK_DIR}/jars/*" \
 org.apache.spark.deploy.SparkSubmit \
 --master local[*] \
 --conf spark.executor.extraJavaOptions=-Duser.timezone=Etc/UTC \
 --conf spark.eventLog.enabled=false \
 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
 --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
 --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
 --name "Thrift JDBC/ODBC Server" \
 --executor-memory 512m \
 --conf spark.sql.warehouse.dir=/spark/delta-tables \
 --packages io.delta:delta-core_2.12:1.1.0 \
 spark-internal