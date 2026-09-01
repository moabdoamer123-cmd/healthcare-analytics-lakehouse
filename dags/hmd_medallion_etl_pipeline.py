import os
import requests
import psycopg2
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default Retries and Scheduling Config
default_args = {
    "owner": "Mo Amer",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email": ["mo.abdo.amer123@gmail.com"],   
    "email_on_failure": True,                  
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}
# -----------------------------------------------------------------------------
# Python Callables for Lightweight Checks & API Triggers
# -----------------------------------------------------------------------------
def run_pre_run_checks():
    """
    Requirement Task 1: 'Verify Neon Postgres connectivity, MinIO server
    health, and bucket availability.'

    FIX: the original version only checked that the env vars were
    non-empty strings — that's configuration presence, not connectivity.
    This actually opens a connection to Neon, hits MinIO's real liveness
    endpoint, and confirms the target bucket exists.
    """
    print("🔍 Executing Pre-run Health Checks...")

    neon_host = os.getenv("NEON_HOST")
    neon_port = os.getenv("NEON_PORT", "5432")
    neon_db = os.getenv("NEON_DB")
    neon_user = os.getenv("NEON_USER")
    neon_password = os.getenv("NEON_PASSWORD")
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    bucket_name = os.getenv("MINIO_BUCKET", "hmd-lakehouse")

    required = {
        "NEON_HOST": neon_host, "NEON_DB": neon_db, "NEON_USER": neon_user,
        "NEON_PASSWORD": neon_password, "MINIO_ENDPOINT": minio_endpoint,
        "MINIO_ACCESS_KEY": minio_access_key, "MINIO_SECRET_KEY": minio_secret_key,
    }
    missing = [name for name, val in required.items() if not val]
    if missing:
        raise ValueError(f"❌ Missing required environment variables: {', '.join(missing)}")

    # 1. Neon Postgres CONNECTIVITY — a real connection + query, not a string check.
    try:
        conn = psycopg2.connect(
            host=neon_host, port=neon_port, dbname=neon_db,
            user=neon_user, password=neon_password,
            sslmode="require", connect_timeout=10,
        )
        conn.cursor().execute("SELECT 1;")
        conn.close()
        print(f"✅ Neon Postgres reachable at {neon_host}")
    except Exception as e:
        raise ConnectionError(f"❌ Neon Postgres connectivity check failed: {e}")

    # 2. MinIO SERVER HEALTH — MinIO's standard liveness probe endpoint.
    try:
        resp = requests.get(f"{minio_endpoint}/minio/health/live", timeout=10)
        if resp.status_code != 200:
            raise ConnectionError(f"unexpected status code {resp.status_code}")
        print(f"✅ MinIO server healthy at {minio_endpoint}")
    except Exception as e:
        raise ConnectionError(f"❌ MinIO health check failed: {e}")

    # 3. BUCKET AVAILABILITY — create target bucket automatically if it doesn't exist
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
        )
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"✅ Bucket '{bucket_name}' exists and is reachable")
        except ClientError:
            print(f"⚠️ Bucket '{bucket_name}' not found. Creating it now...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"✅ Bucket '{bucket_name}' created successfully!")
    except Exception as e:
        raise ConnectionError(f"❌ Failed to verify or create bucket '{bucket_name}': {e}")


def trigger_power_bi_refresh():
    """Requirement Task 6: Trigger Power BI Dataset Refresh via REST API."""
    print("🚀 Initiating Power BI Semantic Model Refresh...")

    tenant_id = os.getenv("POWERBI_TENANT_ID")
    client_id = os.getenv("POWERBI_CLIENT_ID")
    client_secret = os.getenv("POWERBI_CLIENT_SECRET")
    dataset_id = os.getenv("POWERBI_DATASET_ID")
    workspace_id = os.getenv("POWERBI_WORKSPACE_ID")

    if not all([tenant_id, client_id, client_secret, dataset_id]):
        print("⚠️ Power BI Credentials not fully configured in .env. Skipping API call for local dry-run.")
        return

    # 1. Fetch OAuth Access Token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
    }
    token_res = requests.post(token_url, data=token_data, timeout=30)
    token_res.raise_for_status()
    access_token = token_res.json().get("access_token")

    # 2. Trigger Refresh using Group Endpoint (Workspace explicit URL)
    if workspace_id:
        refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    else:
        refresh_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"

    headers = {"Authorization": f"Bearer {access_token}"}
    refresh_res = requests.post(refresh_url, headers=headers, timeout=30)
    refresh_res.raise_for_status()
    print("✅ Power BI Semantic Model refresh triggered successfully!")


# -----------------------------------------------------------------------------
# DAG Pipeline Definition
# -----------------------------------------------------------------------------
with DAG(
    dag_id="hmd_medallion_etl_pipeline",
    default_args=default_args,
    description="Orchestrates End-to-End Healthcare Analytics Lakehouse (Bronze -> Silver -> Gold)",
    schedule_interval="@daily",
    catchup=False,
    tags=["pyspark", "minio", "medallion", "healthcare"],
) as dag:

    # Task 1: Pre-run Check
    t1_pre_run_check = PythonOperator(
        task_id="pre_run_check",
        python_callable=run_pre_run_checks,
    )

    # Task 2: PySpark Extraction (Neon DB -> MinIO Bronze)
    t2_pyspark_extraction = BashOperator(
        task_id="pyspark_extraction",
        bash_command="python /opt/airflow/scripts/01_bronze_ingestion.py",
    )

    # Task 3: Data Quality Rules (Bronze -> Silver & Quarantine)
    # FIX: filename now matches the actual script (was
    # "02_silver_transformation.py", a file that doesn't exist).
    t3_data_quality_rules = BashOperator(
        task_id="data_quality_rules",
        bash_command="python /opt/airflow/scripts/02_silver_ingestion.py",
    )

    # Task 4: Gold Transformations (Silver -> Gold Dimensional Model)
    t4_gold_transformations = BashOperator(
        task_id="gold_transformations",
        bash_command="python /opt/airflow/scripts/03_gold_transformation.py",
    )

    # Task 5: Reconciliation Checks
    # FIX: this used to be a hardcoded `echo` that always printed success
    # no matter what — it now actually runs the real reconciliation script,
    # which raises (and therefore fails this task) if anything doesn't
    # add up. Because tasks are chained with `>>` and the default trigger
    # rule is all_success, a failure here correctly blocks Task 6 below —
    # Power BI never refreshes on data that failed reconciliation.
    t5_reconciliation_checks = BashOperator(
        task_id="reconciliation_checks",
        bash_command="python /opt/airflow/scripts/04_reconciliation_checks.py",
    )

    # Task 6: Power BI Refresh Trigger
    t6_power_bi_refresh = PythonOperator(
        task_id="power_bi_refresh",
        python_callable=trigger_power_bi_refresh,
    )

    # -----------------------------------------------------------------------------
    # Execution Order
    # -----------------------------------------------------------------------------
    (
        t1_pre_run_check
        >> t2_pyspark_extraction
        >> t3_data_quality_rules
        >> t4_gold_transformations
        >> t5_reconciliation_checks
        >> t6_power_bi_refresh
    )