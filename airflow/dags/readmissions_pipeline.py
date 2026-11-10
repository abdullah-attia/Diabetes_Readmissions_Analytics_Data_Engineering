import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

PROJECT = "/opt/project"
JOB = "readmissions_ingestion"


def run_ssis_job():
    """Starts the SQL Agent job (which runs both SSIS packages) and waits for it to finish."""
    hook = MsSqlHook(mssql_conn_id="mssql_staging")
    hook.run(f"EXEC msdb.dbo.sp_start_job @job_name = N'{JOB}';", autocommit=True)
    time.sleep(20)  # give the job time to register as running
    deadline = time.time() + 30 * 60
    while time.time() < deadline:
        df = hook.get_pandas_df(
            f"EXEC msdb.dbo.sp_help_job @job_name = N'{JOB}', @job_aspect = N'JOB';"
        )
        if int(df.loc[0, "current_execution_status"]) == 4:  # 4 = idle, i.e. finished
            if int(df.loc[0, "last_run_outcome"]) != 1:      # 1 = succeeded
                raise RuntimeError("SSIS Agent job failed - check job history in SSMS")
            return
        time.sleep(15)
    raise TimeoutError("SSIS Agent job did not finish within 30 minutes")


with DAG(
    dag_id="readmissions_pipeline",
    description="SSIS -> SQL Server staging -> Spark -> Snowflake -> dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["readmissions"],
) as dag:

    run_ssis = PythonOperator(task_id="run_ssis", python_callable=run_ssis_job)

    check_audit = SQLCheckOperator(
        task_id="check_audit",
        conn_id="mssql_staging",
        sql="""
            SELECT CASE WHEN COUNT(*) = 2
                         AND MIN(CASE WHEN status = 'success' THEN 1 ELSE 0 END) = 1
                        THEN 1 ELSE 0 END
            FROM (SELECT TOP 2 status
                  FROM ReadmissionsStaging.etl.load_audit
                  ORDER BY started_at DESC) last_two
        """,
    )

    run_spark = BashOperator(
        task_id="run_spark",
        bash_command=(
            f"cd {PROJECT} && python cleaning/clean_encounters.py --source jdbc "
            "&& python scripts/verify_outputs.py"
        ),
    )

    load_snowflake = BashOperator(
        task_id="load_snowflake",
        bash_command=f"cd {PROJECT} && python warehouse/load_snowflake.py",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT}/warehouse/dbt_project && "
            "/opt/dbt/bin/dbt seed --profiles-dir . && /opt/dbt/bin/dbt build --profiles-dir ."
        ),
    )

    run_ssis >> check_audit >> run_spark >> load_snowflake >> dbt_build