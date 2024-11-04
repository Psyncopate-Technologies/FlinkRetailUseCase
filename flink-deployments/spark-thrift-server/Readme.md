docker build -t metabase/spark:3.3.2 . #For Dockerfile_old

docker build -t spark-thrift-server .

docker run -d \
  --name spark-thrift-server \
  -p 10000:10000 \
  -v /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/watchlist-trigger/delta-tables:/spark/delta-tables \
  spark-thrift-server

From terminal, launch beeline cli...
export SPARK_HOME="/usr/local/opt/apache-spark/libexec"
beeline -u "jdbc:hive2://localhost:10000"


CREATE TABLE watchlist_trigger_alerts USING delta LOCATION '/spark/delta-tables/watchlist-trigger-alerts';