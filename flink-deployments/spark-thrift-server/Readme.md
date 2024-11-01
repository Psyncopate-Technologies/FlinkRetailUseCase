docker build -t metabase/spark:3.3.2 . #For Dockerfile_old

docker build -t spark-thrift-server .

docker run -p 10000:10000 \
       --name sparksql-3.3.2 \
       --network flink-network \
       --rm \
       -d metabase/spark:3.3.2