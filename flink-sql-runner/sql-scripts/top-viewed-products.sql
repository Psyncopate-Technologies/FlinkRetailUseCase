SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.timeout' = '5m';

CREATE CATALOG delta_catalog
    WITH ('type'         = 'delta-catalog',
          'catalog-type' = 'in-memory');

USE CATALOG delta_catalog;


CREATE DATABASE delta_db;
USE delta_db;

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

CREATE TABLE portal_views (
  product_id STRING,
  user_id STRING,
  page_url STRING,
  view_time INT,
  ip STRING,
  ts TIMESTAMP(3),
  `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'shoe_ecommerce_portal_audit',
  'properties.bootstrap.servers' = '192.168.0.240:29094',
  'properties.group.id' = 'portalviews-consumergroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://192.168.0.240:8081'
);


CREATE TABLE top_viewed_products (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  product_id STRING,
  total_view_time INT,
  cnt BIGINT,
  topN BIGINT,
  PRIMARY KEY (`product_id`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'top_viewed_products_tumbling',
  'properties.bootstrap.servers' = '192.168.0.240:29094',
  'value.format' = 'json',
  'key.format' = 'json'
  --'value.format' = 'avro-confluent',
  --'value.avro-confluent.url' = 'http://192.168.0.240:8081'
);

INSERT INTO top_viewed_products
  SELECT *
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY total_view_time DESC) as topN
      FROM (
        SELECT window_start, window_end, product_id, SUM(view_time) as total_view_time, COUNT(*) as cnt
        FROM TABLE(
          TUMBLE(TABLE portal_views, DESCRIPTOR(event_time), INTERVAL '1' HOUR))
        GROUP BY window_start, window_end, product_id
      )
    ) WHERE topN <= 3;
