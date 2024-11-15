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





-- CLAIM PROCEDURE


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



-- CLAIM PROVIDER

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
  'path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/inputFiles/claim_provider',
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





-- Full Claim Info

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



CREATE TABLE eligible_procedures (
    procedure_code STRING,
    procedure_description STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/StagingFiles/new_eligible_procedures',
    'format' = 'csv'
);

-- Create ineligible_procedures table using filesystem connector
CREATE TABLE ineligible_procedures (
    procedure_code STRING,
    procedure_description STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/StagingFiles/ineligible_procedures',
    'format' = 'csv'
);

CREATE TABLE eligible_diagnosis (
    diagnosis_code STRING,
    diagnosis_description STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/StagingFiles/eligible_diagnosis',
    'format' = 'csv'
);

CREATE TABLE ineligible_diagnosis (
    diagnosis_code STRING,
    diagnosis_description STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/StagingFiles/ineligible_diagnosis',
    'format' = 'csv'
);


CREATE TABLE procedure_thresholds (
    procedure_code STRING,
    cost_threshold DOUBLE
) WITH (
    'connector' = 'filesystem',
    'path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/StagingFiles/procedure_thresholds',
    'format' = 'csv'
);


CREATE TABLE processed_claim_full_info (
    claim_id STRING PRIMARY KEY NOT ENFORCED,
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
    event_time TIMESTAMP,
    adjudicated BOOLEAN
) WITH (
    'connector' = 'delta',
    'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/processed_claim_full_info'
);


CREATE TABLE rejected_claims_delta_table (
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
    event_time TIMESTAMP,
    adjudicated BOOLEAN
) WITH (
    'connector' = 'delta',
    'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/rejected_claims_delta_table'
);