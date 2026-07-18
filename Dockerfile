FROM apache/airflow:2.9.2-python3.11

USER root
#java 17 pro spark 
# adi curl para baixar o driver na build
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# driver jdbc do postgres
RUN mkdir -p /opt/spark_jars && \
    curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
        -o /opt/spark_jars/postgresql-42.7.3.jar && \
    chown -R airflow: /opt/spark_jars

USER airflow
# spark dbt e conector do postgres
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    psycopg2-binary==2.9.9 \
    dbt-postgres==1.7.4
