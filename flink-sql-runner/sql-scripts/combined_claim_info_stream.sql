SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.timeout' = '5m';

CREATE CATALOG delta_catalog
    WITH ('type' = 'delta-catalog',
          'catalog-type' = 'in-memory');

USE CATALOG delta_catalog;

CREATE DATABASE delta_db;
USE delta_db;


CREATE TABLE claim_full_info (
  claim_id STRING,
  member_id STRING,
  diagnosis_code STRING,
  diagnosis_description STRING,
  diagnosis_date TIMESTAMP(3),
  lab_results STRING,
  procedure_code STRING,
  procedure_description STRING,
  procedure_date TIMESTAMP(3),
  procedure_cost DOUBLE,
  provider_id STRING,
  provider_name STRING,
  in_network STRING,
  facility_name STRING,
  event_time TIMESTAMP
) WITH (
  'connector' = 'delta',
  'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/claim_full_info' --ADLS
);

INSERT INTO claim_full_info
SELECT
  d.claim_id,
  d.member_id,
  d.diagnosis_code,
  d.diagnosis_description,
  d.diagnosis_date,
  d.lab_results,
  p.procedure_code,
  p.procedure_description,
  p.procedure_date,
  p.procedure_cost,
  pr.provider_id,
  pr.provider_name,
  pr.in_network,
  pr.facility_name,
  d.event_time
FROM claim_diagnosis_delta_table d
JOIN claim_procedure_delta_table p ON d.claim_id = p.claim_id
JOIN claim_provider_delta_table pr ON d.claim_id = pr.claim_id;


