# Use Flink as the base image
FROM flink:1.19.0-scala_2.12-java11

# Copy the jar from the target folder to the image
COPY ./target/*.jar /opt/flink/lib/
RUN ln -fs /opt/flink/opt/flink-azure-fs-hadoop-*.jar /opt/flink/lib/.