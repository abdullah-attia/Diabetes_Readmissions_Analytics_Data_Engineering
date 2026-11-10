FROM apache/airflow:2.10.5-python3.11

USER root
# Java for Spark, the SQL Server JDBC driver, and dbt in its own virtual environment
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /opt/jars \
 && curl -L -o /opt/jars/mssql-jdbc.jar \
    https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.8.1.jre11/mssql-jdbc-12.8.1.jre11.jar \
 && python -m venv /opt/dbt \
 && /opt/dbt/bin/pip install --no-cache-dir dbt-snowflake \
 && chown -R airflow: /opt/dbt
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
ARG AIRFLOW_VERSION=2.10.5
# Providers and connectors pinned to versions compatible with this Airflow release
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-microsoft-mssql \
      snowflake-connector-python \
      pyarrow \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt" \
 && pip install --no-cache-dir pyspark==3.5.1