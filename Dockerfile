FROM apache/airflow:2.9.2-python3.11

USER root
#java 17 pro spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# spark dbt e conector do postgres
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    psycopg2-binary==2.9.9 \
    dbt-postgres==1.7.4
