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


CREATE TABLE claim_diagnosis_delta_table (
  claim_id STRING,
  member_id STRING,
  diagnosis_code STRING,
  diagnosis_description STRING,
  diagnosis_date TIMESTAMP_LTZ(3),
  lab_results STRING,
  event_time TIMESTAMP,
) WITH (
  'connector' = 'delta',
  'table-path' = 'file:///mount/claim_diagnosis_delta_table'
);


  CREATE TABLE retrieved_claim_diagnosis_delta_table WITH (
  'connector' = 'filesystem',
  'path' = 'file:///outputFiles/retrieved_claim_diagnosis_delta_table',
  'format' = 'csv'
);

INSERT INTO retrieved_claim_diagnosis_delta_table SELECT claim_id, member_id, diagnosis_code, diagnosis_description, diagnosis_date, lab_results, event_time FROM claim_diagnosis_delta_table /*+ OPTIONS('mode' = 'streaming') */;
