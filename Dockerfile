FROM apache/airflow:2.10.3-python3.10
USER root
RUN apt-get update && apt-get install -y default-jre-headless wget zip unzip
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.0 psycopg2-binary

# Download Iceberg dependencies for Spark 3.5
RUN mkdir -p /opt/airflow/iceberg-jars && \
    cd /opt/airflow/iceberg-jars && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.0/iceberg-spark-runtime-3.5_2.12-1.4.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/1.4.0/iceberg-core-1.4.0.jar