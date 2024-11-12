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

CREATE TABLE input_claim_diagnosis (
  claim_id STRING,
  member_id STRING,
  diagnosis_code STRING,
  diagnosis_description STRING,
  diagnosis_date TIMESTAMP(3),
  lab_results STRING,
  `event_time` TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'file.modification-time'
) WITH (
  'connector' = 'filesystem',
  'source.monitor-interval' = '5',
  --'path' = 'file:///inputFiles/claim_diagnosis', --ADLS
  'path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/inputFiles/claim_diagnosis', --ADLS
  'format' = 'csv'
);

CREATE TABLE raw_claim_diagnosis_delta_table (
  claim_id STRING,
  member_id STRING,
  diagnosis_code STRING,
  diagnosis_description STRING,
  diagnosis_date TIMESTAMP(3),
  lab_results STRING,
  event_time TIMESTAMP
)
WITH (
    'connector' = 'delta',
    --'table-path' = 'file:///mount/claim_diagnosis_delta_table' --ADLS
    'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/claim_diagnosis' --ADLS
    --'fs.azure.account.key.molinahealthcareusecase.dfs.core.windows.net' = '<AZ STORAGE_ACCOUNT ACCESS KEY>'
    --'table-path' = 'wasb://molina@molinahealthcareusecase.blob.core.windows.net/claim_diagnosis'
);

CREATE TABLE input_claim_procedure (
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


CREATE TABLE raw_claim_procedure_delta_table (
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

CREATE TABLE input_claim_provider (
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



CREATE TABLE raw_claim_provider_delta_table (
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