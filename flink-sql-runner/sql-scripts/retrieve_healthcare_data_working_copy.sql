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

CREATE TABLE process_claim1_delta_table (
    id STRING,
    name STRING,
    event_time TIMESTAMP
  ) WITH (
    'connector' = 'delta',
    'table-path' = 'file:///mount/process_claim1_delta_table'
);


  CREATE TABLE retrieved_claim1_table (
  id STRING,
  name STRING,
  event_time TIMESTAMP
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///outputFiles',
  'format' = 'csv'
);

INSERT INTO retrieved_claim1_table SELECT id, name, event_time FROM process_claim1_delta_table /*+ OPTIONS('mode' = 'streaming') */;
