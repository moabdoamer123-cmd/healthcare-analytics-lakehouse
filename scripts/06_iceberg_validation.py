#!/usr/bin/env python3
"""
Iceberg Integration Validation Script

Validates after Iceberg migration:
1. Row count reconciliation (Bronze vs Iceberg)
2. Schema preservation
3. Data quality checks
4. File storage verification

Run after 02_silver_ingestion_iceberg.py has completed
"""

import os
import sys
from pyspark.sql import SparkSession

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

SCHEMA_NAME = "HMD"
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "glacier_iceberg")
BRONZE_PATH = "s3a://hmd-lakehouse/bronze"
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://hmd-lakehouse/iceberg/warehouse")


def setup_spark():
    """Initialize Spark with Iceberg support."""
    spark = (
        SparkSession.builder
        .appName("Iceberg_Validation")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_row_counts(spark):
    """Compare Bronze Parquet vs Iceberg row counts."""
    print("\n" + "=" * 80)
    print("VALIDATION STEP 1: ROW COUNT RECONCILIATION")
    print("=" * 80)

    tables = ["patients", "doctors", "appointments", "treatments", "billing"]

    all_match = True

    for table in tables:
        print(f"\n🔍 Checking {table}...")

        # Read from Bronze
        try:
            bronze_df = spark.read.parquet(f"{BRONZE_PATH}/{table}")
            bronze_count = bronze_df.count()
        except Exception as e:
            print(f"   ❌ Bronze read failed: {e}")
            all_match = False
            continue

        # Read from Iceberg
        try:
            iceberg_df = spark.sql(f"SELECT COUNT(*) AS cnt FROM {ICEBERG_CATALOG}.{SCHEMA_NAME}.{table}")
            iceberg_count = iceberg_df.collect()[0]["cnt"]
        except Exception as e:
            print(f"   ❌ Iceberg read failed: {e}")
            all_match = False
            continue

        # Silver sends rejected rows to quarantine, so Bronze == Iceberg + Quarantine
        quarantine_path = f"s3a://hmd-lakehouse/quarantine/failed_records/{table}"
        try:
            q_count = spark.read.parquet(quarantine_path).count()
        except Exception:
            q_count = 0

        match = bronze_count == iceberg_count + q_count
        status = "✅ PASS" if match else "❌ FAIL"
        print(f"   Bronze:     {bronze_count:,}")
        print(f"   Iceberg:    {iceberg_count:,}")
        print(f"   Quarantine: {q_count:,}")
        print(f"   Result:     {status}")
        all_match = all_match and match

    print(f"\n{'=' * 80}")
    if all_match:
        print("✅ ROW COUNT RECONCILIATION: ALL TABLES PASSED")
    else:
        print("❌ ROW COUNT RECONCILIATION: SOME TABLES FAILED")
    print("=" * 80)

    return all_match


def validate_schema(spark):
    """Compare schema between Bronze and Iceberg."""
    print("\n" + "=" * 80)
    print("VALIDATION STEP 2: SCHEMA PRESERVATION")
    print("=" * 80)

    tables = ["patients", "appointments"]

    schema_match = True

    for table in tables:
        print(f"\n🔍 Schema comparison: {table}")

        # Get Bronze schema
        bronze_df = spark.read.parquet(f"{BRONZE_PATH}/{table}")
        bronze_schema = bronze_df.schema

        # Get Iceberg schema
        iceberg_df = spark.sql(f"DESCRIBE {ICEBERG_CATALOG}.{SCHEMA_NAME}.{table}")
        ice_columns = {row["col_name"] for row in iceberg_df.collect()}
        bronze_columns = set(bronze_schema.fieldNames())

        # Check column presence
        missing = bronze_columns - ice_columns
        extra = ice_columns - bronze_columns

        if not missing:
            print(f"   ✅ All Bronze columns preserved in Iceberg ({len(bronze_columns)} columns)")
            print(f"   Columns: {', '.join(sorted(bronze_columns))}")
            if extra:
                print(f"   ℹ️  Extra in Iceberg ({len(extra)}, added by Silver): {', '.join(sorted(extra))}")
        else:
            print(f"   ❌ Schema mismatch:")
            print(f"      Missing from Iceberg: {', '.join(sorted(missing))}")
            if extra:
                print(f"      Extra in Iceberg: {', '.join(sorted(extra))}")

        schema_match = schema_match and not missing

    print(f"\n{'=' * 80}")
    print("VALIDATION RESULT:", "✅ PASS" if schema_match else "❌ FAIL")
    print("=" * 80)

    return schema_match


def validate_data_quality(spark):
    """Validate key data quality metrics."""
    print("\n" + "=" * 80)
    print("VALIDATION STEP 3: DATA QUALITY")
    print("=" * 80)

    # Check appointments for critical fields
    try:
        df = spark.sql(f"""
            SELECT
                COUNT(*) AS total,
                COUNT(DISTINCT patient_id) AS unique_patients,
                COUNT(DISTINCT doctor_id) AS unique_doctors,
                COUNT(*) - COUNT(DISTINCT appointment_id) AS duplicates
            FROM {ICEBERG_CATALOG}.{SCHEMA_NAME}.appointments
        """)
        result = df.collect()[0]

        print(f"\n   Appointments table quality:")
        print(f"      Total records:          {result['total']:,}")
        print(f"      Unique patients:        {result['unique_patients']:,}")
        print(f"      Unique doctors:         {result['unique_doctors']:,}")
        print(f"      Duplicates:             {result['duplicates']:,}")

        dq_pass = result['duplicates'] == 0
        print(f"\n   Result: {'✅ PASS' if dq_pass else '❌ FAIL (duplicates found)'}")

    except Exception as e:
        print(f"❌ Data quality check failed: {e}")
        dq_pass = False

    print(f"\n{'=' * 80}")
    print("VALIDATION RESULT:", "✅ PASS" if dq_pass else "❌ FAIL")
    print("=" * 80)

    return dq_pass


def verify_iceberg_files(spark):
    """Verify Iceberg files are actually being stored."""
    print("\n" + "=" * 80)
    print("VALIDATION STEP 4: STORAGE VERIFICATION")
    print("=" * 80)

    # Check for Iceberg table metadata
    try:
        # List Iceberg tables inside the HMD namespace
        tables_df = spark.sql(f"SHOW TABLES IN {ICEBERG_CATALOG}.{SCHEMA_NAME}")
        ice_tables = {row["tableName"] for row in tables_df.collect()}
        print(f"\n   Iceberg tables: {', '.join(sorted(ice_tables))}")

        # Check that our target tables exist
        expected_tables = ["appointments", "patients", "doctors", "treatments", "billing"]
        missing = [t for t in expected_tables if t not in ice_tables]

        if missing:
            print(f"   ❌ Missing tables: {', '.join(missing)}")
            storage_ok = False
        else:
            print(f"   ✅ All expected tables present in Iceberg catalog")
            storage_ok = True

    except Exception as e:
        print(f"❌ Storage verification failed: {e}")
        import traceback
        traceback.print_exc()
        storage_ok = False

    print(f"\n{'=' * 80}")
    print("VALIDATION RESULT:", "✅ PASS" if storage_ok else "❌ FAIL")
    print("=" * 80)

    return storage_ok


def validate_partitioning(spark):
    """Verify Iceberg table metadata (files + snapshots) is readable."""
    print("\n" + "=" * 80)
    print("VALIDATION STEP 5: ICEBERG TABLE METADATA")
    print("=" * 80)

    metadata_ok = True

    for table in ["patients", "doctors", "appointments", "treatments", "billing"]:
        print(f"\n🔍 Metadata: {table}")
        try:
            spark.sql(
                f"SELECT COUNT(*) AS files FROM {ICEBERG_CATALOG}.{SCHEMA_NAME}.{table}.files"
            ).show()
            spark.sql(
                f"SELECT * FROM {ICEBERG_CATALOG}.{SCHEMA_NAME}.{table}.snapshots"
            ).show(truncate=False)
        except Exception as e:
            print(f"   ❌ Could not read Iceberg metadata for {table}: {e}")
            metadata_ok = False

    print(f"\n{'=' * 80}")
    print("VALIDATION RESULT:", "✅ PASS" if metadata_ok else "❌ FAIL")
    print("=" * 80)

    return metadata_ok


def main():
    """Run all validation tests."""
    print("\n" + "=" * 80)
    print("📋 ICEBERG INTEGRATION VALIDATION SUITE")
    print("Project: Healthcare Analytics Lakehouse")
    print("=" * 80)

    spark = setup_spark()

    try:
        results = {}

        # Run all validations
        results['row_counts'] = validate_row_counts(spark)
        results['schema'] = validate_schema(spark)
        results['data_quality'] = validate_data_quality(spark)
        results['storage'] = verify_iceberg_files(spark)
        results['partitioning'] = validate_partitioning(spark)

        # Final summary
        print("\n" + "=" * 80)
        print("FINAL VALIDATION SUMMARY")
        print("=" * 80)

        total = len(results)
        passed = sum(1 for v in results.values() if v)

        for test_name, passed_flag in results.items():
            icon = "✅" if passed_flag else "❌"
            print(f"{icon} {test_name}: {'PASS' if passed_flag else 'FAIL'}")

        print(f"\n{'=' * 80}")
        print(f"OVERALL RESULT: {passed}/{total} tests passed")
        print("=" * 80)

        if passed == total:
            print("\n🎉 All validations passed! Iceberg integration is successful.")
            return 0
        else:
            print(f"\n⚠️  {total - passed} validation(s) failed. Please review logs.")
            return 1

    except Exception as e:
        print(f"\n❌ Validation suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        spark.stop()
        print("\n🚀 Spark session closed.")


if __name__ == "__main__":
    sys.exit(main())