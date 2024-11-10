FROM flink:1.19

RUN mkdir /opt/flink/usrlib
ADD target/flink-sql-runner-*.jar /opt/flink/usrlib/sql-runner.jar
ADD sql-scripts /opt/flink/usrlib/sql-scripts
#ADD hadoop-azure-3.3.2.jar /opt/flink/lib/hadoop-azure-3.3.2.jar
#ADD hadoop-azure-datalake-3.3.2.jar /opt/flink/lib/hadoop-azure-datalake-3.3.2.jar
#ADD delta-flink-3.2.1.jar /opt/flink/lib/delta-flink-3.2.1.jar
#ADD delta-standalone_2.12-3.2.1.jar /opt/flink/lib/delta-standalone_2.12-3.2.1.jar
ADD delta-storage-3.2.1.jar /opt/flink/lib/delta-storage-3.2.1.jar
RUN ln -fs /opt/flink/opt/flink-azure-fs-hadoop-*.jar /opt/flink/lib/.
