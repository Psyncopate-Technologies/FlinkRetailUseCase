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



CREATE TABLE claim_provider (
  claim_id STRING,
  provider_id STRING,
  provider_name STRING,
  in_network STRING,
  facility_name STRING,
  `event_time` TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'file.modification-time'
) WITH (
  'connector' = 'filesystem',
  'source.monitor-interval' = '5',
  --'path' = 'file:///inputFiles/claim_provider',
  'path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/inputFiles/claim_provider', --ADLS
  'format' = 'csv'
);



CREATE TABLE claim_provider_delta_table (
  claim_id STRING,
  provider_id STRING,
  provider_name STRING,
  in_network STRING,
  facility_name STRING,
  event_time TIMESTAMP
)WITH (
    'connector' = 'delta',
    --'table-path' = 'file:///mount/claim_provider_delta_table'
    'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/claim_provider' --ADLS
);



INSERT INTO claim_provider_delta_table SELECT claim_id, provider_id, provider_name, in_network, facility_name, event_time FROM claim_provider;