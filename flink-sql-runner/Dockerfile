FROM flink:1.19

RUN mkdir -p /opt/flink/usrlib
RUN mkdir -p /opt/hive/conf

ADD target/flink-sql-runner-*.jar /opt/flink/usrlib/sql-runner.jar
ADD sql-scripts /opt/flink/usrlib/sql-scripts
ADD hive-site.xml /opt/hive/conf/hive-site.xml

#ADD hadoop-azure-3.3.2.jar /opt/flink/lib/hadoop-azure-3.3.2.jar
#ADD hadoop-azure-datalake-3.3.2.jar /opt/flink/lib/hadoop-azure-datalake-3.3.2.jar
#ADD delta-flink-3.2.1.jar /opt/flink/lib/delta-flink-3.2.1.jar
#ADD delta-standalone_2.12-3.2.1.jar /opt/flink/lib/delta-standalone_2.12-3.2.1.jar
#ADD delta-storage-3.2.1.jar /opt/flink/lib/delta-storage-3.2.1.jar
RUN curl -L -o /opt/flink/lib/flink-table-api-java-bridge-1.17.2.jar https://repo1.maven.org/maven2/org/apache/flink/flink-table-api-java-bridge/1.17.2/flink-table-api-java-bridge-1.17.2.jar
RUN curl -L -o /opt/flink/lib/flink-table-common-1.17.2.jar https://repo1.maven.org/maven2/org/apache/flink/flink-table-common/1.17.2/flink-table-common-1.17.2.jar
RUN curl -L -o /opt/flink/lib/flink-parquet-1.17.2.jar https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.17.2/flink-parquet-1.17.2.jar
# RUN curl -L -o /opt/flink/lib/flink-table-runtime-1.17.2.jar https://repo1.maven.org/maven2/org/apache/flink/flink-table-runtime/1.17.2/flink-table-runtime-1.17.2.jar
RUN curl -L -o /opt/flink/lib/delta-storage-3.2.1.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar
RUN curl -L -o /opt/flink/lib/flink-connector-hive_2.12-1.17.2.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-hive_2.12/1.17.2/flink-connector-hive_2.12-1.17.2.jar
RUN curl -L -o /opt/flink/lib/flink-sql-connector-hive-2.3.9_2.12-1.17.2.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.9_2.12/1.17.2/flink-sql-connector-hive-2.3.9_2.12-1.17.2.jar
RUN curl -L -o /opt/flink/lib/hadoop-common-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.2/hadoop-common-3.3.2.jar
RUN curl -L -o /opt/flink/lib/hadoop-client-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/3.3.2/hadoop-client-3.3.2.jar
RUN curl -L -o /opt/flink/lib/hadoop-mapreduce-client-core-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.2/hadoop-mapreduce-client-core-3.3.2.jar
RUN curl -L -o /opt/flink/lib/hadoop-mapreduce-client-jobclient-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-jobclient/3.3.2/hadoop-mapreduce-client-jobclient-3.3.2.jar
RUN curl -L -o /opt/flink/lib/hadoop-mapreduce-client-common-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-common/3.3.2/hadoop-mapreduce-client-common-3.3.2.jar
RUN curl -L -o /opt/flink/lib/hive-common-2.3.9.jar https://repo1.maven.org/maven2/org/apache/hive/hive-common/2.3.9/hive-common-2.3.9.jar
RUN curl -L -o /opt/flink/lib/hive-metastore-2.3.9.jar https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/2.3.9/hive-metastore-2.3.9.jar
RUN curl -L -o /opt/flink/lib/hive-exec-2.3.9.jar https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar
RUN curl -L -o /opt/flink/lib/javax.servlet-api-4.0.1.jar https://repo1.maven.org/maven2/javax/servlet/javax.servlet-api/4.0.1/javax.servlet-api-4.0.1.jar
RUN curl -L -o /opt/flink/lib/libfb303-0.9.3.jar https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar
RUN curl -L -o /opt/flink/lib/libthrift-0.21.0.jar https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.21.0/libthrift-0.21.0.jar
RUN ln -fs /opt/flink/opt/flink-azure-fs-hadoop-*.jar /opt/flink/lib/.
