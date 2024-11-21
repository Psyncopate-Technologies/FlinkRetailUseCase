# Stage 1: Download the JARs
FROM ubuntu:20.04 AS downloader

# Install curl or wget in this temporary stage
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download the JAR files
RUN curl -L -o /tmp/delta-storage-3.2.1.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar
RUN curl -L -o /tmp/flink-connector-files-1.19.1.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-files/1.19.1/flink-connector-files-1.19.1.jar


#FROM flink:1.19
FROM confluentinc/cp-flink:1.19.1-cp1

RUN mkdir /opt/flink/usrlib
ADD target/flink-sql-runner-*.jar /opt/flink/usrlib/sql-runner.jar
ADD sql-scripts /opt/flink/usrlib/sql-scripts
#ADD hadoop-azure-3.3.2.jar /opt/flink/lib/hadoop-azure-3.3.2.jar
#ADD hadoop-azure-datalake-3.3.2.jar /opt/flink/lib/hadoop-azure-datalake-3.3.2.jar
#ADD delta-flink-3.2.1.jar /opt/flink/lib/delta-flink-3.2.1.jar
#ADD delta-standalone_2.12-3.2.1.jar /opt/flink/lib/delta-standalone_2.12-3.2.1.jar

# Copy the JARs from the downloader stage
COPY --from=downloader /tmp/delta-storage-3.2.1.jar /opt/flink/lib/delta-storage-3.2.1.jar
COPY --from=downloader /tmp/flink-connector-files-1.19.1.jar /opt/flink/lib/flink-connector-files-1.19.1.jar

#ADD delta-storage-3.2.1.jar /opt/flink/lib/delta-storage-3.2.1.jar
#ADD flink-connector-files-1.19.1.jar /opt/flink/lib/flink-connector-files-1.19.1.jar

#RUN dnf update && dnf install -y curl
#RUN curl -L -o /opt/flink/lib/delta-storage-3.2.1.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar
#RUN wget --output-document=/opt/flink/lib/delta-storage-3.2.1.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar
RUN ln -fs /opt/flink/opt/flink-azure-fs-hadoop-*.jar /opt/flink/lib/.