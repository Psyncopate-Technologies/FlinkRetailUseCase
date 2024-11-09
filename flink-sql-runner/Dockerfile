FROM flink:1.19

RUN mkdir /opt/flink/usrlib
ADD target/flink-sql-runner-*.jar /opt/flink/usrlib/sql-runner.jar
ADD sql-scripts /opt/flink/usrlib/sql-scripts
RUN ln -fs /opt/flink/opt/flink-azure-fs-hadoop-*.jar /opt/flink/lib/.
