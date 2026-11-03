/* ============================================================
   verify_ingestion.sql - Member A
   One health check for the whole ingestion step.
   Every row should say PASS.
   ============================================================ */
USE ReadmissionsStaging;
GO

WITH checks AS (
    -- 1. Data loaded
    SELECT 1 AS n, 'Encounters in staging = 101,766' AS check_name,
           CAST((SELECT COUNT(*) FROM stg.encounters_raw) AS NVARCHAR(50)) AS actual,
           CASE WHEN (SELECT COUNT(*) FROM stg.encounters_raw) = 101766 THEN 'PASS' ELSE 'FAIL' END AS result
    UNION ALL
    SELECT 2, 'No duplicate encounter_id',
           CAST((SELECT COUNT(DISTINCT encounter_id) FROM stg.encounters_raw) AS NVARCHAR(50)),
           CASE WHEN (SELECT COUNT(DISTINCT encounter_id) FROM stg.encounters_raw) = 101766 THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    SELECT 3, 'Distinct patients = 71,518',
           CAST((SELECT COUNT(DISTINCT patient_nbr) FROM stg.encounters_raw) AS NVARCHAR(50)),
           CASE WHEN (SELECT COUNT(DISTINCT patient_nbr) FROM stg.encounters_raw) = 71518 THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    SELECT 4, 'Mapping codes = 63 (8 + 30 + 25)',
           CAST((SELECT COUNT(*) FROM stg.id_mapping) AS NVARCHAR(50)),
           CASE WHEN (SELECT COUNT(*) FROM stg.id_mapping) = 63
                 AND (SELECT COUNT(*) FROM stg.id_mapping WHERE mapping_type = 'admission_type') = 8
                 AND (SELECT COUNT(*) FROM stg.id_mapping WHERE mapping_type = 'discharge_disposition') = 30
                 AND (SELECT COUNT(*) FROM stg.id_mapping WHERE mapping_type = 'admission_source') = 25
                THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    -- 2. Raw values kept exactly as in the file
    SELECT 5, 'Raw "?" kept (weight = 98,569)',
           CAST((SELECT COUNT(*) FROM stg.encounters_raw WHERE weight = '?') AS NVARCHAR(50)),
           CASE WHEN (SELECT COUNT(*) FROM stg.encounters_raw WHERE weight = '?') = 98569 THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    SELECT 6, 'readmitted has exactly 3 values',
           CAST((SELECT COUNT(DISTINCT readmitted) FROM stg.encounters_raw) AS NVARCHAR(50)),
           CASE WHEN (SELECT COUNT(DISTINCT readmitted) FROM stg.encounters_raw) = 3 THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    -- 3. Every row traceable to one load
    SELECT 7, 'Every staging row has a load_id',
           CAST((SELECT COUNT(*) FROM stg.encounters_raw WHERE load_id IS NULL) AS NVARCHAR(50)) + ' missing',
           CASE WHEN (SELECT COUNT(*) FROM stg.encounters_raw WHERE load_id IS NULL) = 0
                 AND (SELECT COUNT(*) FROM stg.id_mapping WHERE load_id IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    -- 4. Audit proves nothing was lost
    SELECT 8, 'Last encounters load: read = written, success',
           (SELECT TOP 1 CONCAT(rows_read, ' / ', rows_written, ' / ', status) FROM etl.load_audit
            WHERE source_file LIKE '%diabetic_data.csv' ORDER BY load_id DESC),
           CASE WHEN EXISTS (SELECT 1 FROM (SELECT TOP 1 * FROM etl.load_audit WHERE source_file LIKE '%diabetic_data.csv' ORDER BY load_id DESC) a
                             WHERE a.rows_read = a.rows_written AND a.rows_written = 101766 AND a.status = 'success') THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    SELECT 9, 'Last mapping load: read = written, success',
           (SELECT TOP 1 CONCAT(rows_read, ' / ', rows_written, ' / ', status) FROM etl.load_audit
            WHERE source_file LIKE '%IDs_mapping.csv' ORDER BY load_id DESC),
           CASE WHEN EXISTS (SELECT 1 FROM (SELECT TOP 1 * FROM etl.load_audit WHERE source_file LIKE '%IDs_mapping.csv' ORDER BY load_id DESC) a
                             WHERE a.rows_read = a.rows_written AND a.rows_written = 63 AND a.status = 'success') THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    SELECT 10, 'No bad rows in the last load',
           CAST((SELECT COUNT(*) FROM etl.load_errors
                 WHERE load_id = (SELECT MAX(load_id) FROM stg.encounters_raw)) AS NVARCHAR(50)),
           CASE WHEN (SELECT COUNT(*) FROM etl.load_errors
                      WHERE load_id = (SELECT MAX(load_id) FROM stg.encounters_raw)) = 0 THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    -- 5. Deployed and runnable for Airflow
    SELECT 11, 'Both packages deployed to SSISDB',
           CAST((SELECT COUNT(*) FROM SSISDB.catalog.packages p
                 JOIN SSISDB.catalog.projects pr ON pr.project_id = p.project_id
                 JOIN SSISDB.catalog.folders f ON f.folder_id = pr.folder_id
                 WHERE f.name = 'Readmissions') AS NVARCHAR(50)),
           CASE WHEN (SELECT COUNT(*) FROM SSISDB.catalog.packages p
                      JOIN SSISDB.catalog.projects pr ON pr.project_id = p.project_id
                      JOIN SSISDB.catalog.folders f ON f.folder_id = pr.folder_id
                      WHERE f.name = 'Readmissions') >= 2 THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    SELECT 12, 'Last SSISDB run succeeded (status 7)',
           (SELECT TOP 1 CONCAT(package_name, ' = ', status) FROM SSISDB.catalog.executions ORDER BY execution_id DESC),
           CASE WHEN (SELECT TOP 1 status FROM SSISDB.catalog.executions ORDER BY execution_id DESC) = 7 THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    -- 6. Security and logging
    SELECT 13, 'spark_reader can SELECT on stg',
           'SELECT on schema stg',
           CASE WHEN EXISTS (SELECT 1 FROM sys.database_permissions dp
                             JOIN sys.database_principals u ON u.principal_id = dp.grantee_principal_id
                             WHERE u.name = 'spark_reader' AND dp.permission_name = 'SELECT' AND dp.state = 'G') THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    SELECT 14, 'spark_reader cannot DELETE',
           'no DELETE grant',
           CASE WHEN NOT EXISTS (SELECT 1 FROM sys.database_permissions dp
                                 JOIN sys.database_principals u ON u.principal_id = dp.grantee_principal_id
                                 WHERE u.name = 'spark_reader' AND dp.permission_name IN ('DELETE', 'INSERT', 'UPDATE', 'CONTROL')) THEN 'PASS' ELSE 'FAIL' END
    UNION ALL
    SELECT 15, 'SSISDB logging on (Basic or higher)',
           (SELECT CAST(property_value AS NVARCHAR(50)) FROM SSISDB.catalog.catalog_properties WHERE property_name = 'SERVER_LOGGING_LEVEL'),
           CASE WHEN (SELECT CAST(property_value AS INT) FROM SSISDB.catalog.catalog_properties WHERE property_name = 'SERVER_LOGGING_LEVEL') >= 1 THEN 'PASS' ELSE 'FAIL' END
)
SELECT n, check_name, actual, result FROM checks ORDER BY n;

-- Summary line
-- (re-run the block above and count: all 15 should be PASS)
