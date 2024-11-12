FROM flink:1.19

RUN mkdir /opt/flink/usrlib
#ADD hadoop-azure-3.3.2.jar /opt/flink/lib/hadoop-azure-3.3.2.jar
#ADD hadoop-azure-datalake-3.3.2.jar /opt/flink/lib/hadoop-azure-datalake-3.3.2.jar
ADD delta-lake-jars/delta-flink-3.2.1.jar /opt/flink/lib/delta-flink-3.2.1.jar
ADD delta-lake-jars/delta-standalone_2.12-3.2.1.jar /opt/flink/lib/delta-standalone_2.12-3.2.1.jar
ADD delta-lake-jars/delta-storage-3.2.1.jar /opt/flink/lib/delta-storage-3.2.1.jar
ADD delta-lake-jars/flink-parquet-1.19.0.jar /opt/flink/lib/flink-parquet-1.19.0.jar
ADD delta-lake-jars/parquet-hadoop-1.13.1.jar /opt/flink/lib/parquet-hadoop-1.13.1.jar
ADD delta-lake-jars/parquet-common-1.13.1.jar /opt/flink/lib/parquet-common-1.13.1.jar
ADD delta-lake-jars/parquet-jackson-1.13.1.jar /opt/flink/lib/parquet-jackson-1.13.1.jar
ADD delta-lake-jars/jackson-core-2.15.2.jar /opt/flink/lib/jackson-core-2.15.2.jar
ADD delta-lake-jars/jackson-databind-2.15.2.jar /opt/flink/lib/jackson-databind-2.15.2.jar
ADD delta-lake-jars/jackson-annotations-2.15.2.jar /opt/flink/lib/jackson-annotations-2.15.2.jar
#ADD delta-lake-jars/hadoop-azure-3.3.2.jar /opt/flink/lib/hadoop-azure-3.3.2.jar
#ADD delta-lake-jars/hadoop-azure-datalake-3.3.2.jar /opt/flink/lib/hadoop-azure-datalake-3.3.2.jar
#ADD delta-lake-jars/hadoop-common-3.3.2.jar /opt/flink/lib/hadoop-common-3.3.2.jar
ADD delta-lake-jars/hadoop-client-3.3.2.jar /opt/flink/lib/hadoop-client-3.3.2.jar
ADD delta-lake-jars/shapeless_2.12-2.3.4.jar /opt/flink/lib/shapeless_2.12-2.3.4.jar
RUN ln -fs /opt/flink/opt/flink-azure-fs-hadoop-*.jar /opt/flink/lib/.
