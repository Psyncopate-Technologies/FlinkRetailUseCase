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



CREATE TABLE claim_procedure (
  claim_id STRING,
  member_id STRING,
  procedure_code STRING,
  procedure_description STRING,
  procedure_date TIMESTAMP(3),
  procedure_cost DOUBLE,
  `event_time` TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'file.modification-time'
) WITH (
  'connector' = 'filesystem',
  'source.monitor-interval' = '5',
  --'path' = 'file:///inputFiles/claim_procedure',
  'path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/inputFiles/claim_procedure', --ADLS
  'format' = 'csv'
);





CREATE TABLE claim_procedure_delta_table (
  claim_id STRING,
  member_id STRING,
  procedure_code STRING,
  procedure_description STRING,
  procedure_date TIMESTAMP(3),
  procedure_cost DOUBLE,
  event_time TIMESTAMP
)
WITH (
    'connector' = 'delta',
    --'table-path' = 'file:///mount/claim_procedure_delta_table'
    'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/claim_procedure' --ADLS
);


INSERT INTO claim_procedure_delta_table SELECT claim_id, member_id, procedure_code, procedure_description, procedure_date, procedure_cost, event_time FROM claim_procedure;