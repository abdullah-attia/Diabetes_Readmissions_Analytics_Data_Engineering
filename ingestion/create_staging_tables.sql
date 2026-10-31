/* ============================================================
   create_staging_tables.sql - Member A (Day 1)
   Creates the staging, audit and error tables.
   Safe to re-run: drops and recreates the tables.
   ============================================================ */
USE ReadmissionsStaging;
GO

IF SCHEMA_ID('stg') IS NULL EXEC('CREATE SCHEMA stg');
IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl');
GO

DROP TABLE IF EXISTS stg.encounters_raw;
DROP TABLE IF EXISTS stg.id_mapping;
DROP TABLE IF EXISTS etl.load_audit;
DROP TABLE IF EXISTS etl.load_errors;
GO

-- 1. Raw encounters: all 50 columns as text, exactly as in the file
CREATE TABLE stg.encounters_raw (
    encounter_id               NVARCHAR(255),
    patient_nbr                NVARCHAR(255),
    race                       NVARCHAR(255),
    gender                     NVARCHAR(255),
    age                        NVARCHAR(255),
    weight                     NVARCHAR(255),
    admission_type_id          NVARCHAR(255),
    discharge_disposition_id   NVARCHAR(255),
    admission_source_id        NVARCHAR(255),
    time_in_hospital           NVARCHAR(255),
    payer_code                 NVARCHAR(255),
    medical_specialty          NVARCHAR(255),
    num_lab_procedures         NVARCHAR(255),
    num_procedures             NVARCHAR(255),
    num_medications            NVARCHAR(255),
    number_outpatient          NVARCHAR(255),
    number_emergency           NVARCHAR(255),
    number_inpatient           NVARCHAR(255),
    diag_1                     NVARCHAR(255),
    diag_2                     NVARCHAR(255),
    diag_3                     NVARCHAR(255),
    number_diagnoses           NVARCHAR(255),
    max_glu_serum              NVARCHAR(255),
    A1Cresult                  NVARCHAR(255),
    metformin                  NVARCHAR(255),
    repaglinide                NVARCHAR(255),
    nateglinide                NVARCHAR(255),
    chlorpropamide             NVARCHAR(255),
    glimepiride                NVARCHAR(255),
    acetohexamide              NVARCHAR(255),
    glipizide                  NVARCHAR(255),
    glyburide                  NVARCHAR(255),
    tolbutamide                NVARCHAR(255),
    pioglitazone               NVARCHAR(255),
    rosiglitazone              NVARCHAR(255),
    acarbose                   NVARCHAR(255),
    miglitol                   NVARCHAR(255),
    troglitazone               NVARCHAR(255),
    tolazamide                 NVARCHAR(255),
    examide                    NVARCHAR(255),
    citoglipton                NVARCHAR(255),
    insulin                    NVARCHAR(255),
    [glyburide-metformin]      NVARCHAR(255),
    [glipizide-metformin]      NVARCHAR(255),
    [glimepiride-pioglitazone] NVARCHAR(255),
    [metformin-rosiglitazone]  NVARCHAR(255),
    [metformin-pioglitazone]   NVARCHAR(255),
    [change]                   NVARCHAR(255),
    diabetesMed                NVARCHAR(255),
    readmitted                 NVARCHAR(255),
    -- added by the load
    load_id                    INT,
    loaded_at                  DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- 2. ID mapping: the 3 stacked tables from IDs_mapping.csv
CREATE TABLE stg.id_mapping (
    mapping_type  NVARCHAR(50),   -- admission_type / discharge_disposition / admission_source
    id            NVARCHAR(10),
    description   NVARCHAR(255),
    load_id       INT,
    loaded_at     DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- 3. Audit: one row per load run
CREATE TABLE etl.load_audit (
    load_id       INT IDENTITY(1,1) PRIMARY KEY,
    source_file   NVARCHAR(260),
    rows_read     INT,
    rows_written  INT,
    started_at    DATETIME2,
    finished_at   DATETIME2,
    status        NVARCHAR(20)    -- running / success / failed
);
GO

-- 4. Error rows: bad rows go here instead of stopping the load
CREATE TABLE etl.load_errors (
    error_id      INT IDENTITY(1,1) PRIMARY KEY,
    load_id       INT,
    source_file   NVARCHAR(260),
    raw_line      NVARCHAR(MAX),
    error_code    INT,
    error_column  INT,
    logged_at     DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Check: expect 4 tables (encounters_raw = 52 columns)
SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) AS columns
FROM INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_TYPE = 'BASE TABLE'
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
GO
