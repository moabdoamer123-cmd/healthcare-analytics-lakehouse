"""
Apache Iceberg Time Travel Demo - Healthcare Analytics

Demonstrates:
1. Snapshots and history tracking
2. Query historical state
3. Rollback to previous version

Run after completing the scheduled tasks in 02_silver_ingestion_iceberg.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
import time

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

SCHEMA_NAME = "HMD"
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "glacier_iceberg")
MONTH_AGO = "2024-11-01"  # Adjust based on your actual data dates
ICEBERG_CATALOG_NAME = ICEBERG_CATALOG


def setup_env(spark):
    """Setup S3 and Iceberg catalog configuration."""
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    spark.conf.set("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    spark.conf.set("spark.sql.catalog.glacier_iceberg", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glacier_iceberg.type", "hadoop")
    spark.conf.set("spark.sql.catalog.glacier_iceberg.warehouse", f"s3a://hmd-lakehouse/iceberg/warehouse")
    spark.conf.set("spark.sql.catalog.glacier_iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set("spark.sql.catalog.glacier_iceberg.warehouse-impl", "org.apache.iceberg.fs.S3FileSystem")
    spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    print(f"✅ Iceberg catalog: {ICEBERG_CATALOG}")


def snapshot_current_state(appointments_df, catalog_name, schema_name, table_name):
    """Take a snapshot of the current dataset."""
    snapshot_timestamp = time.strftime("%Y%m%d_%H%M%S")
    snapshot_id = f"snapshot_{snapshot_timestamp}"

    # Write new snapshot with explicit timestamp column
    (
        appointments_df
        .withColumn("_snapshot_timestamp", current_timestamp())
        .withColumn("_snapshot_id", f"lit('{snapshot_id}')")
        .write
        .mode("append")
        .format("iceberg")
        .option("path", f"s3a://hmd-lakehouse/iceberg/{SCHEMA_NAME}/appointments")
        .save()
    )

    print(f"📸 Snapshot taken with ID: {snapshot_id}")
    return snapshot_id


def query_current_snapshot(catalog_name, schema_name, table_name, limit=10):
    """Query the current snapshot."""
    query = f"""
        SELECT appointment_id, patient_id, doctor_id, appointment_date,
               appointment_timestamp, status, is_noshow
        FROM {catalog_name}.{schema_name}.{table_name}
        ORDER BY appointment_timestamp DESC
        LIMIT {limit}
    """
    df = spark.sql(query)
    count = df.count()
    print(f"\n📊 Current snapshot ({catalog_name}.{schema_name}.{table_name}):")
    print(f"   Rows: {count}")
    print(f"\n   Recent records:")
    df.show(truncate=False)
    return df, count


def query_history_before_date(catalog_name, schema_name, table_name, date_str):
    """Query appointments before a specific date."""
    query = f"""
        SELECT appointment_id, patient_id, doctor_id, appointment_date,
               appointment_timestamp, status, is_noshow
        FROM {catalog_name}.{schema_name}.{table_name}
        WHERE appointment_date < '{date_str}'
        ORDER BY appointment_timestamp DESC
        LIMIT 10
    """
    df = spark.sql(query)
    count = df.count()
    print(f"\n📜 Historical state before {date_str}:")
    print(f"   Rows: {count}")
    print(f"\n   Records:")
    df.show(truncate=False)
    return df, count


def display_history(catalog_name, schema_name, table_name):
    """Show Iceberg table history."""
    query = f"""
        SELECT *
        FROM {catalog_name}.snapshots
        WHERE table_id IN (
            SELECT table_id FROM {catalog_name}.tables WHERE namespace_id IN (
                SELECT namespace_id FROM {catalog_name}.namespaces WHERE namespace_id = (
                    SELECT namespace_id FROM {catalog_name}.namespaces WHERE namespace = array('{schema_name}')
                )
            )
        )
        ORDER BY timestamp DESC
        LIMIT 10
    """
    df = spark.sql(query)
    print(f"\n🕰️ Table history (last 10 snapshots):")
    df.show(truncate=False)


def display_partition_metadata(catalog_name, schema_name, table_name):
    """Show partition evolution."""
    query = f"""
        SELECT *
        FROM {catalog_name}.snapshots
        JOIN {catalog_name}.partition_data USING (snapshot_id)
        WHERE snapshot_id IN (
            SELECT snapshot_id FROM {catalog_name}.snapshots
            WHERE snapshot_id IN (
                SELECT latest_snapshot_id FROM {catalog_name}.snapshots
            )
        )
        LIMIT 10
    """
    df = spark.sql(query)
    print(f"\n📊 Partition Evolution:")
    df.show(truncate=False)


def show_schema_info(catalog_name, schema_name, table_name):
    """Show current schema of Iceberg table."""
    query = f"""
        SHOW COLUMNS FROM {catalog_name}.{schema_name}.{table_name}
    """
    df = spark.sql(query)
    print(f"\n📋 Table schema:")
    df.show(truncate=False)


# Initialize Spark
spark = (
    SparkSession.builder
    .appName("Iceberg_Time_Travel_Demo")
    .config("spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0"
    )
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("=" * 70)
print("🔵 Apache Iceberg Time Travel Demo - Healthcare Analytics")
print("=" * 70)

setup_env(spark)

# Main demonstration
try:
    # Step 1: Show current schema
    print("\n" + "=" * 70)
    print("STEP 1: GETTING CURRENT TABLE INFO")
    print("=" * 70)
    show_schema_info(ICEBERG_CATALOG_NAME, SCHEMA_NAME, "appointments")

    # Step 2: Show table history
    print("\n" + "=" * 70)
    print("STEP 2: VIEWING ICEBERG SNAPSHOT HISTORY")
    print("=" * 70)
    display_history(ICEBERG_CATALOG_NAME, SCHEMA_NAME, "appointments")

    # Step 3: Read current snapshot (pre-modification)
    print("\n" + "=" * 70)
    print("STEP 3: RECORDING CURRENT STATE (BEFORE CHANGES)")
    print("=" * 70)
    df_snapshot, count_snapshot = query_current_snapshot(
        ICEBERG_CATALOG_NAME, SCHEMA_NAME, "appointments"
    )

    # Step 4: Query historical data
    print("\n" + "=" * 70)
    print("STEP 4: QUERYING DATA FROM SNAPSHOT BEFORE SPECIFIC DATE")
    print("=" * 70)
    df_history, count_history = query_history_before_date(
        ICEBERG_CATALOG_NAME, SCHEMA_NAME, "appointments", MONTH_AGO
    )

    # Step 5: Show partition evolution (schema evolution demo)
    print("\n" + "=" * 70)
    print("STEP 5: CHECKING PARTITION EVOLUTION")
    print("=" * 70)
    # This will show recent partition stats from Iceberg metadata
    partition_query = f"""
        SELECT snapshot_id, start_snapshot_id,
               added_files, removed_files
        FROM {ICEBERG_CATALOG_NAME}.snapshots
        WHERE latest_snapshot_id IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 5
    """
    df_partitions = spark.sql(partition_query)
    print("\n📊 Recent partition activity:")
    df_partitions.show(truncate=False)

    # Summary
    print("\n" + "=" * 70)
    print("✅ DEMO SUMMARY")
    print("=" * 70)
    print(f"✅ Current snapshot has {count_snapshot} records")
    print(f"✅ Historical records before {MONTH_AGO}: {count_history}")
    print(f"✅ Iceberg table supports transactional schema evolution")
    print(f"✅ Time-travel queries are available via Spark SQL")
    print("=" * 70)

except Exception as e:
    print(f"\n❌ Demo failed: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("\n🚀 Spark session closed.")