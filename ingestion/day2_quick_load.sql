/* ============================================================
   Day 2 - Quick test load (Member A)
   Loads both raw CSVs into staging with BULK INSERT so profiling
   can start today. On Day 3 the SSIS packages replace this load.

   Project folder: D:\projects\readmissions-pipeline
   ============================================================ */
USE ReadmissionsStaging;
GO

/* ---------- 1. A view with only the 50 file columns ----------
   The table has 2 extra columns (load_id, loaded_at), so BULK INSERT
   goes through this view, which matches the file column-for-column. */
CREATE OR ALTER VIEW stg.v_encounters_file AS
SELECT encounter_id, patient_nbr, race, gender, age, weight,
       admission_type_id, discharge_disposition_id, admission_source_id,
       time_in_hospital, payer_code, medical_specialty,
       num_lab_procedures, num_procedures, num_medications,
       number_outpatient, number_emergency, number_inpatient,
       diag_1, diag_2, diag_3, number_diagnoses,
       max_glu_serum, A1Cresult,
       metformin, repaglinide, nateglinide, chlorpropamide, glimepiride,
       acetohexamide, glipizide, glyburide, tolbutamide, pioglitazone,
       rosiglitazone, acarbose, miglitol, troglitazone, tolazamide,
       examide, citoglipton, insulin,
       [glyburide-metformin], [glipizide-metformin],
       [glimepiride-pioglitazone], [metformin-rosiglitazone],
       [metformin-pioglitazone],
       [change], diabetesMed, readmitted
FROM stg.encounters_raw;
GO

/* ---------- 2. Load diabetic_data.csv ---------- */
DECLARE @folder NVARCHAR(260) = N'D:\projects\readmissions-pipeline\data\raw\';
DECLARE @file   NVARCHAR(260) = @folder + N'diabetic_data.csv';
DECLARE @load_id INT, @sql NVARCHAR(MAX);

TRUNCATE TABLE stg.encounters_raw;

INSERT INTO etl.load_audit (source_file, started_at, status)
VALUES (@file, SYSDATETIME(), N'running');
SET @load_id = SCOPE_IDENTITY();

SET @sql = N'BULK INSERT stg.v_encounters_file
FROM ''' + @file + N'''
WITH (FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','',
      ROWTERMINATOR = ''0x0a'', CODEPAGE = ''65001'', TABLOCK);';
EXEC sp_executesql @sql;

-- Remove a Windows line-ending character (CR) from the last column, if present
UPDATE stg.encounters_raw
SET readmitted = REPLACE(readmitted, CHAR(13), N''),
    load_id    = @load_id;

UPDATE etl.load_audit
SET rows_read    = (SELECT COUNT(*) FROM stg.encounters_raw),
    rows_written = (SELECT COUNT(*) FROM stg.encounters_raw),
    finished_at  = SYSDATETIME(),
    status       = N'success'
WHERE load_id = @load_id;

SELECT COUNT(*) AS encounters_loaded FROM stg.encounters_raw;   -- expect 101,766
GO

/* ---------- 3. Load IDs_mapping.csv (3 tables stacked in 1 file) ----------
   Load every line into a work table, keeping file order (line_no),
   then tag each row with the section it belongs to. */
/* Each whole line is loaded into ONE column (no CSV parsing),
   then split at the first comma in SQL. This copes with labels
   that contain commas or quotes. */
DROP VIEW  IF EXISTS etl.v_id_mapping_lines;
DROP TABLE IF EXISTS etl.id_mapping_lines;
CREATE TABLE etl.id_mapping_lines (
    line_no     INT IDENTITY(1,1),
    raw_line    NVARCHAR(1000),
    col1        NVARCHAR(255),
    col2        NVARCHAR(1000)
);
GO
CREATE OR ALTER VIEW etl.v_id_mapping_lines AS
SELECT raw_line FROM etl.id_mapping_lines;
GO

DECLARE @folder NVARCHAR(260) = N'D:\projects\readmissions-pipeline\data\raw\';
DECLARE @file   NVARCHAR(260) = @folder + N'IDs_mapping.csv';
DECLARE @load_id INT, @sql NVARCHAR(MAX);

INSERT INTO etl.load_audit (source_file, started_at, status)
VALUES (@file, SYSDATETIME(), N'running');
SET @load_id = SCOPE_IDENTITY();

-- FIELDTERMINATOR is a character that never appears, so each line = 1 value
SET @sql = N'BULK INSERT etl.v_id_mapping_lines
FROM ''' + @file + N'''
WITH (FIRSTROW = 1, FIELDTERMINATOR = ''0x07'',
      ROWTERMINATOR = ''0x0a'', CODEPAGE = ''65001'');';
EXEC sp_executesql @sql;

-- Remove CR, split at the first comma, strip surrounding quotes
UPDATE etl.id_mapping_lines
SET raw_line = REPLACE(REPLACE(raw_line, CHAR(13), N''), NCHAR(65279), N'');  -- CR + byte-order mark

UPDATE etl.id_mapping_lines
SET col1 = LTRIM(RTRIM(CASE WHEN CHARINDEX(N',', raw_line) > 0
                            THEN LEFT(raw_line, CHARINDEX(N',', raw_line) - 1)
                            ELSE raw_line END)),
    col2 = CASE WHEN CHARINDEX(N',', raw_line) > 0
                THEN SUBSTRING(raw_line, CHARINDEX(N',', raw_line) + 1, 1000)
                ELSE N'' END;

UPDATE etl.id_mapping_lines
SET col2 = SUBSTRING(col2, 2, LEN(col2) - 2)
WHERE LEFT(col2, 1) = N'"' AND RIGHT(col2, 1) = N'"' AND LEN(col2) >= 2;

TRUNCATE TABLE stg.id_mapping;

WITH tagged AS (
    SELECT l.line_no, l.col1, l.col2,
           -- the most recent header line above this row tells us the section
           (SELECT TOP 1 h.col1 FROM etl.id_mapping_lines h
            WHERE h.line_no <= l.line_no
              AND h.col1 IN (N'admission_type_id', N'discharge_disposition_id', N'admission_source_id')
            ORDER BY h.line_no DESC) AS header
    FROM etl.id_mapping_lines l
)
INSERT INTO stg.id_mapping (mapping_type, id, description, load_id)
SELECT REPLACE(header, N'_id', N''), col1, col2, @load_id
FROM tagged
WHERE col1 NOT IN (N'admission_type_id', N'discharge_disposition_id', N'admission_source_id')
  AND col1 IS NOT NULL AND col1 <> N'';           -- drop header + blank lines

UPDATE etl.load_audit
SET rows_read    = (SELECT COUNT(*) FROM etl.id_mapping_lines),
    rows_written = (SELECT COUNT(*) FROM stg.id_mapping),
    finished_at  = SYSDATETIME(),
    status       = N'success'
WHERE load_id = @load_id;

-- Expect 3 mapping types (8, 30 and 25 rows in the standard file)
SELECT mapping_type, COUNT(*) AS ids FROM stg.id_mapping GROUP BY mapping_type;
SELECT * FROM etl.load_audit ORDER BY load_id;
GO
