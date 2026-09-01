FROM apache/airflow:2.8.1-python3.10
USER root
RUN apt-get update && apt-get install -y default-jre-headless
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.0 psycopg2-binary