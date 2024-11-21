# Stage 1: Download the JARs
FROM ubuntu:20.04 AS downloader

# Install curl or wget in this temporary stage
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download the JAR files
RUN curl -L -o /tmp/flink-connector-files-1.19.1.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-files/1.19.1/flink-connector-files-1.19.1.jar

# Use Flink as the base image
#FROM flink:1.19.0-scala_2.12-java11
FROM confluentinc/cp-flink:1.19.1-cp1

# Copy the jar from the target folder to the image
COPY ./target/*.jar /opt/flink/lib/
#ADD flink-connector-files-1.19.1.jar /opt/flink/lib/flink-connector-files-1.19.1.jar
# Copy the JARs from the downloader stage
COPY --from=downloader /tmp/flink-connector-files-1.19.1.jar /opt/flink/lib/flink-connector-files-1.19.1.jar

RUN ln -fs /opt/flink/opt/flink-azure-fs-hadoop-*.jar /opt/flink/lib/.