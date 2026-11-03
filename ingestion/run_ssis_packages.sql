/* ============================================================
   run_ssis_packages.sql - Member A
   Runs both SSIS packages from SSISDB and waits for each to finish
   (SYNCHRONIZED = 1). This is the call the Airflow run_ssis task uses.
   Status: 7 = succeeded, 4 = failed.
   ============================================================ */
DECLARE @exec_id BIGINT;

-- 1. load_encounters
EXEC SSISDB.catalog.create_execution
     @folder_name     = N'Readmissions',
     @project_name    = N'ReadmissionsIngestion',
     @package_name    = N'load_encounters.dtsx',
     @use32bitruntime = 0,
     @execution_id    = @exec_id OUTPUT;
EXEC SSISDB.catalog.set_execution_parameter_value @exec_id,
     @object_type = 50, @parameter_name = N'SYNCHRONIZED', @parameter_value = 1;
EXEC SSISDB.catalog.start_execution @exec_id;
SELECT 'load_encounters' AS package, execution_id, status
FROM SSISDB.catalog.executions WHERE execution_id = @exec_id;

-- 2. load_id_mappings
EXEC SSISDB.catalog.create_execution
     @folder_name     = N'Readmissions',
     @project_name    = N'ReadmissionsIngestion',
     @package_name    = N'load_id_mappings.dtsx',
     @use32bitruntime = 0,
     @execution_id    = @exec_id OUTPUT;
EXEC SSISDB.catalog.set_execution_parameter_value @exec_id,
     @object_type = 50, @parameter_name = N'SYNCHRONIZED', @parameter_value = 1;
EXEC SSISDB.catalog.start_execution @exec_id;
SELECT 'load_id_mappings' AS package, execution_id, status
FROM SSISDB.catalog.executions WHERE execution_id = @exec_id;

-- 3. Audit check (the Airflow check_audit task)
SELECT TOP 2 load_id, source_file, rows_read, rows_written, status
FROM ReadmissionsStaging.etl.load_audit ORDER BY load_id DESC;

-- If a status is 4, see why:
-- SELECT TOP 10 message_time, message FROM SSISDB.catalog.event_messages
-- WHERE event_name = 'OnError' ORDER BY message_time DESC;
