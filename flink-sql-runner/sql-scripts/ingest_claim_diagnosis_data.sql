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

CREATE TABLE claim_diagnosis (
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



CREATE TABLE claim_diagnosis_delta_table (
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
    --'table-path' = 'wasb://molina@molinahealthcareusecase.blob.core.windows.net/claim_diagnosis'
);




--INSERT INTO shoes_delta_table VALUES ('a', 'b', 'c');
INSERT INTO claim_diagnosis_delta_table SELECT claim_id, member_id, diagnosis_code, diagnosis_description, diagnosis_date, lab_results, event_time FROM claim_diagnosis;
