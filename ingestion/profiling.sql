/* ============================================================
   profiling.sql - Member A
   One query per data-quality issue (plan section 5).
   Run each block, then copy the numbers into docs/issues_register.md
   ============================================================ */
USE ReadmissionsStaging;
GO

/* ---------- ALL: row counts and keys ---------- */
SELECT COUNT(*)                     AS rows_in_staging,        -- expect 101,766
       COUNT(DISTINCT encounter_id) AS distinct_encounters,    -- should equal rows
       COUNT(DISTINCT patient_nbr)  AS distinct_patients       -- expect ~71,518
FROM stg.encounters_raw;

SELECT load_id, source_file, rows_read, rows_written, status
FROM etl.load_audit ORDER BY load_id DESC;
GO

/* ---------- ISSUES 1 + 2: "?" per column (missing values) ---------- */
DECLARE @parts NVARCHAR(MAX), @sql NVARCHAR(MAX);
SELECT @parts = STRING_AGG(CAST(
    N'SELECT ''' + COLUMN_NAME + N''' AS column_name, ' +
    N'SUM(CASE WHEN ' + QUOTENAME(COLUMN_NAME) + N' = ''?'' THEN 1 ELSE 0 END) AS question_marks, ' +
    N'SUM(CASE WHEN ' + QUOTENAME(COLUMN_NAME) + N' IS NULL OR ' + QUOTENAME(COLUMN_NAME) + N' = '''' THEN 1 ELSE 0 END) AS blanks, ' +
    N'COUNT(*) AS total_rows FROM stg.encounters_raw'
    AS NVARCHAR(MAX)), N' UNION ALL ')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = 'encounters_raw'
  AND COLUMN_NAME NOT IN ('load_id', 'loaded_at');

SET @sql = N'SELECT column_name, question_marks, blanks, total_rows,
       CAST(100.0 * question_marks / total_rows AS DECIMAL(5,2)) AS pct_question_mark
FROM (' + @parts + N') x
WHERE question_marks > 0 OR blanks > 0
ORDER BY pct_question_mark DESC;';
EXEC sp_executesql @sql;
GO

/* ---------- ISSUE 3: codes without labels ---------- */
-- How many IDs each mapping type has
SELECT mapping_type, COUNT(*) AS ids
FROM stg.id_mapping GROUP BY mapping_type;

-- IDs used in encounters that have NO label in the mapping file
SELECT 'admission_type' AS mapping_type, e.admission_type_id AS id, COUNT(*) AS encounters
FROM stg.encounters_raw e
LEFT JOIN stg.id_mapping m ON m.mapping_type = 'admission_type' AND m.id = e.admission_type_id
WHERE m.id IS NULL GROUP BY e.admission_type_id
UNION ALL
SELECT 'discharge_disposition', e.discharge_disposition_id, COUNT(*)
FROM stg.encounters_raw e
LEFT JOIN stg.id_mapping m ON m.mapping_type = 'discharge_disposition' AND m.id = e.discharge_disposition_id
WHERE m.id IS NULL GROUP BY e.discharge_disposition_id
UNION ALL
SELECT 'admission_source', e.admission_source_id, COUNT(*)
FROM stg.encounters_raw e
LEFT JOIN stg.id_mapping m ON m.mapping_type = 'admission_source' AND m.id = e.admission_source_id
WHERE m.id IS NULL GROUP BY e.admission_source_id;
GO

/* ---------- ISSUE 4: many ways to say "unknown" ---------- */
-- Every label that really means "unknown", and how many encounters use it
WITH enc AS (
    SELECT 'admission_type' AS mapping_type, admission_type_id AS id FROM stg.encounters_raw
    UNION ALL SELECT 'discharge_disposition', discharge_disposition_id FROM stg.encounters_raw
    UNION ALL SELECT 'admission_source', admission_source_id FROM stg.encounters_raw
)
SELECT m.mapping_type, m.id, m.description, COUNT(enc.id) AS encounters
FROM stg.id_mapping m
LEFT JOIN enc ON enc.mapping_type = m.mapping_type AND enc.id = m.id
WHERE m.description IS NULL
   OR LTRIM(RTRIM(m.description)) IN ('', 'NULL', 'Not Available', 'Not Mapped', 'Unknown/Invalid')
   OR m.description LIKE '%unknown%'
   OR m.description LIKE '%not mapped%'
GROUP BY m.mapping_type, m.id, m.description
ORDER BY m.mapping_type, encounters DESC;
GO

/* ---------- ISSUE 5: death and hospice discharges ---------- */
SELECT e.discharge_disposition_id AS id,
       m.description,
       COUNT(*) AS encounters,
       CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct,
       CASE WHEN e.discharge_disposition_id IN ('11','13','14','19','20','21')
            THEN 'DEATH / HOSPICE' ELSE '' END AS flag
FROM stg.encounters_raw e
LEFT JOIN stg.id_mapping m
       ON m.mapping_type = 'discharge_disposition' AND m.id = e.discharge_disposition_id
GROUP BY e.discharge_disposition_id, m.description
ORDER BY encounters DESC;

-- Total that cannot be readmitted
SELECT COUNT(*) AS death_or_hospice_encounters
FROM stg.encounters_raw
WHERE discharge_disposition_id IN ('11','13','14','19','20','21');
GO

/* ---------- ISSUE 6: repeat patients ---------- */
WITH per_patient AS (
    SELECT patient_nbr, COUNT(*) AS n FROM stg.encounters_raw GROUP BY patient_nbr
)
SELECT CASE WHEN n = 1 THEN '1' WHEN n = 2 THEN '2'
            WHEN n <= 5 THEN '3-5' ELSE '6+' END AS encounters_per_patient,
       COUNT(*) AS patients,
       SUM(n)   AS encounters
FROM per_patient
GROUP BY CASE WHEN n = 1 THEN '1' WHEN n = 2 THEN '2'
              WHEN n <= 5 THEN '3-5' ELSE '6+' END
ORDER BY MIN(n);

-- Most frequent patient
SELECT TOP 5 patient_nbr, COUNT(*) AS encounters
FROM stg.encounters_raw GROUP BY patient_nbr ORDER BY encounters DESC;
GO

/* ---------- ISSUE 7: raw diagnosis codes ---------- */
WITH dx AS (
    SELECT d.code
    FROM stg.encounters_raw e
    CROSS APPLY (VALUES (e.diag_1), (e.diag_2), (e.diag_3)) d(code)
)
SELECT COUNT(DISTINCT code)                                   AS distinct_codes,
       COUNT(DISTINCT CASE WHEN code LIKE 'V%' THEN code END) AS distinct_v_codes,
       COUNT(DISTINCT CASE WHEN code LIKE 'E%' THEN code END) AS distinct_e_codes,
       SUM(CASE WHEN code LIKE 'V%' THEN 1 ELSE 0 END)        AS v_code_values,
       SUM(CASE WHEN code LIKE 'E%' THEN 1 ELSE 0 END)        AS e_code_values,
       SUM(CASE WHEN code = '?' THEN 1 ELSE 0 END)            AS question_mark_values,
       SUM(CASE WHEN code LIKE '250%' THEN 1 ELSE 0 END)      AS diabetes_250_values
FROM dx;
GO

/* ---------- ISSUE 8: invalid / awkward values ---------- */
SELECT gender, COUNT(*) AS encounters FROM stg.encounters_raw GROUP BY gender ORDER BY encounters DESC;
SELECT age,    COUNT(*) AS encounters FROM stg.encounters_raw GROUP BY age    ORDER BY age;
GO

/* ---------- ISSUE 9: "None" means "not tested" ---------- */
SELECT 'A1Cresult' AS test, ISNULL(A1Cresult, '<NULL>') AS value, COUNT(*) AS encounters
FROM stg.encounters_raw GROUP BY A1Cresult
UNION ALL
SELECT 'max_glu_serum', ISNULL(max_glu_serum, '<NULL>'), COUNT(*)
FROM stg.encounters_raw GROUP BY max_glu_serum
ORDER BY test, encounters DESC;
-- Note: if "None" shows as blank or <NULL> instead of the word None,
-- record that in the register - Member B must treat it as "Not tested".
GO

/* ---------- ISSUE 10: wide medication columns ---------- */
DECLARE @parts NVARCHAR(MAX), @sql NVARCHAR(MAX);
SELECT @parts = STRING_AGG(CAST(
    N'SELECT ''' + c.name + N''' AS medication, ' +
    N'COUNT(DISTINCT ' + QUOTENAME(c.name) + N') AS distinct_values, ' +
    N'SUM(CASE WHEN ' + QUOTENAME(c.name) + N' = ''No'' THEN 1 ELSE 0 END) AS no_count, ' +
    N'SUM(CASE WHEN ' + QUOTENAME(c.name) + N' <> ''No'' THEN 1 ELSE 0 END) AS used_count ' +
    N'FROM stg.encounters_raw'
    AS NVARCHAR(MAX)), N' UNION ALL ')
FROM (VALUES ('metformin'),('repaglinide'),('nateglinide'),('chlorpropamide'),
             ('glimepiride'),('acetohexamide'),('glipizide'),('glyburide'),
             ('tolbutamide'),('pioglitazone'),('rosiglitazone'),('acarbose'),
             ('miglitol'),('troglitazone'),('tolazamide'),('examide'),
             ('citoglipton'),('insulin'),('glyburide-metformin'),
             ('glipizide-metformin'),('glimepiride-pioglitazone'),
             ('metformin-rosiglitazone'),('metformin-pioglitazone')) c(name);

SET @sql = N'SELECT medication, distinct_values, no_count, used_count,
       CASE WHEN distinct_values = 1 THEN ''CONSTANT''
            WHEN used_count < 100   THEN ''NEARLY CONSTANT'' ELSE '''' END AS flag
FROM (' + @parts + N') x ORDER BY used_count;';
EXEC sp_executesql @sql;
GO

/* ---------- BONUS: target column (for the benchmark check) ---------- */
SELECT readmitted, COUNT(*) AS encounters,
       CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
FROM stg.encounters_raw GROUP BY readmitted;
GO
