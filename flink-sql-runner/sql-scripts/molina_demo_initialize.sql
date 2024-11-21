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
  diagnosis_code STRING,
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
  diagnosis_code STRING,
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


--GOLD ADJUDICATED TABLE
CREATE TABLE adjudicated_claims (
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
    'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/adjudicated_claims'
);

-- GOLD UNADJUDICATED TABLE
CREATE TABLE unadjudicated_claims (
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
    'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/unadjudicated_claims'
);

CREATE TABLE adjudicated_claims_summary (
    claim_id STRING PRIMARY KEY NOT ENFORCED,
    total_diagnoses BIGINT,
    total_procedures BIGINT
) WITH (
    'connector' = 'delta',
    'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/adjudicated_claims_summary'
);

CREATE TABLE unadjudicated_claims_summary (
    claim_id STRING,
    total_diagnoses BIGINT,
    total_procedures BIGINT
) WITH (
    'connector' = 'delta',
    'table-path' = 'abfss://molina@molinahealthcareusecase.dfs.core.windows.net/unadjudicated_claims_summary'
);

SET 'pipeline.name' = 'claim_diagnosis_bronze_ingestion';
INSERT INTO claim_diagnosis_delta_table SELECT claim_id, member_id, diagnosis_code, diagnosis_description, diagnosis_date, lab_results, event_time FROM claim_diagnosis;

SET 'pipeline.name' = 'claim_procedure_bronze_ingestion';
INSERT INTO claim_procedure_delta_table SELECT claim_id, member_id, diagnosis_code, procedure_code, procedure_description, procedure_date, procedure_cost, event_time FROM claim_procedure;

SET 'pipeline.name' = 'claim_provider_bronze_ingestion';
INSERT INTO claim_provider_delta_table SELECT claim_id, provider_id, provider_name, in_network, facility_name, event_time FROM claim_provider;

SET 'pipeline.name' = 'eligible_procedures_static_data';
INSERT INTO eligible_procedures VALUES
('PRC001', 'Appendectomy'),
('PRC002', 'Cataract Surgery'),
('PRC003', 'Dental Filling'),
('PRC004', 'Orthopedic Surgery'),
('PRC005', 'Cardiac Surgery'),
('PRC006', 'Stroke Treatment'),
('PRC007', 'Chemotherapy'),
('PRC008', 'ENT Surgery'),
('PRC009', 'Hypertension Treatment'),
('PRC010', 'Asthma Treatment');

SET 'pipeline.name' = 'ineligible_procedures_static_data';
INSERT INTO ineligible_procedures VALUES
('PRC011', 'Blood Transfusion'),
('PRC012', 'Chickenpox Vaccination'),
('PRC013', 'Measles Vaccination'),
('PRC014', 'Mumps Vaccination'),
('PRC015', 'Malaria Treatment'),
('PRC016', 'Typhoid Treatment'),
('PRC017', 'Dengue Treatment'),
('PRC018', 'Cholera Treatment'),
('PRC019', 'Leprosy Treatment'),
('PRC020', 'Tuberculosis Treatment'),
('PRC021', 'Hepatitis Treatment'),
('PRC022', 'Influenza Treatment'),
('PRC023', 'Pneumonia Treatment'),
('PRC024', 'Polio Vaccination'),
('PRC025', 'Rabies Vaccination'),
('PRC026', 'Scabies Treatment'),
('PRC027', 'Smallpox Vaccination'),
('PRC028', 'Tetanus Vaccination'),
('PRC029', 'Whooping Cough Vaccination'),
('PRC030', 'Zika Virus Treatment'),
('PRC031', 'Ebola Treatment'),
('PRC032', 'SARS Treatment'),
('PRC033', 'MERS Treatment'),
('PRC034', 'HIV/AIDS Treatment'),
('PRC035', 'Chikungunya Treatment'),
('PRC036', 'Yellow Fever Vaccination'),
('PRC037', 'West Nile Virus Treatment'),
('PRC038', 'Avian Influenza Treatment'),
('PRC039', 'Swine Flu Treatment');

SET 'pipeline.name' = 'eligible_diagnosis_static_data';
INSERT INTO eligible_diagnosis VALUES
('DG001', 'Appendicitis'),
('DG002', 'Cataract'),
('DG003', 'Tooth Decay'),
('DG004', 'Fracture'),
('DG005', 'Heart Attack'),
('DG006', 'Stroke'),
('DG007', 'Cancer'),
('DG008', 'Sinusitis'),
('DG009', 'Hypertension'),
('DG010', 'Asthma');

SET 'pipeline.name' = 'ineligible_diagnosis_static_data';
INSERT INTO ineligible_diagnosis VALUES
('DG011', 'Anemia'),
('DG012', 'Chickenpox'),
('DG013', 'Measles'),
('DG014', 'Mumps'),
('DG015', 'Malaria'),
('DG016', 'Typhoid'),
('DG017', 'Dengue'),
('DG018', 'Cholera'),
('DG019', 'Leprosy'),
('DG020', 'Tuberculosis'),
('DG021', 'Hepatitis'),
('DG022', 'Influenza'),
('DG023', 'Pneumonia'),
('DG024', 'Polio'),
('DG025', 'Rabies'),
('DG026', 'Scabies'),
('DG027', 'Smallpox'),
('DG028', 'Tetanus'),
('DG029', 'Whooping Cough'),
('DG030', 'Zika Virus'),
('DG031', 'Ebola'),
('DG032', 'SARS'),
('DG033', 'MERS'),
('DG034', 'HIV/AIDS'),
('DG035', 'Chikungunya'),
('DG036', 'Yellow Fever'),
('DG037', 'West Nile Virus'),
('DG038', 'Avian Influenza'),
('DG039', 'Swine Flu');

SET 'pipeline.name' = 'procedure_thresholds_static_data';
INSERT INTO procedure_thresholds VALUES
('PRC001', 1000.00),
('PRC002', 2000.00),
('PRC003', 3500.00),
('PRC004', 3000.00),
('PRC005', 5500.00),
('PRC006', 4500.00),
('PRC007', 6500.00),
('PRC008', 1500.00),
('PRC009', 1200.00),
('PRC010', 2500.00);