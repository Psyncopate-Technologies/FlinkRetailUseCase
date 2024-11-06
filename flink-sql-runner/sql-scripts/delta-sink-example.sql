SET 'execution.checkpointing.interval' = '10s';

-- Configure additional checkpoint options as needed
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE'; -- or 'AT_LEAST_ONCE'
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION'; -- keeps checkpoints if job is canceled
SET 'execution.checkpointing.timeout' = '5m'; -- maximum time for checkpoints

CREATE CATALOG delta_catalog
    WITH ('type'         = 'delta-catalog',
          'catalog-type' = 'in-memory');

USE CATALOG delta_catalog;


CREATE DATABASE delta_db;
USE delta_db;

/* CREATE TABLE shoes (
  _id STRING,
  id STRING,
  brand STRING,
  PRIMARY KEY(_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb-cdc',
  'hosts' = '192.168.0.240:27017/api_prod_db',
  'username' = 'api_user',
  'password' = 'api1234',
  'database' = 'api_prod_db',
  'collection' = 'shoes'
); */

/* CREATE TABLE shoes (
  _id STRING,
  id STRING,
  brand STRING,
  PRIMARY KEY(_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb',
  'uri' = 'mongodb://api_user:api1234@192.168.0.240:27017/api_prod_db',
  'database' = 'api_prod_db',
  'collection' = 'shoes'
); */
CREATE TABLE shoes (
  id STRING,
  brand STRING,
  name STRING,
  sale_price INT,
  rating DOUBLE,
  quantity STRING,
  `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = 'shoes',
  'properties.bootstrap.servers' = '192.168.0.240:29094',
  'properties.group.id' = 'shoes-consumergroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://192.168.0.240:8081'
);




CREATE TABLE shoes_delta_table (
    id STRING,
    brand STRING,
    name STRING,
    sale_price INT,
    rating DOUBLE,
    quantity STRING,
    event_timestamp TIMESTAMP
  ) WITH (
    'connector' = 'delta',
    'table-path' = 'file:///mount/shoes_delta_table'
);

--INSERT INTO shoes_delta_table VALUES ('a', 'b', 'c');
INSERT INTO shoes_delta_table SELECT * FROM shoes;
