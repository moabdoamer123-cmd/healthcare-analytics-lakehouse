import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.functions import current_timestamp, year, month, dayofmonth

# Environment Variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

NEON_HOST = os.getenv(
    "NEON_HOST", "ep-tiny-sunset-ah3xbklk-pooler.c-3.us-east-1.aws.neon.tech"
)
NEON_PORT = os.getenv("NEON_PORT")
NEON_DB = os.getenv("NEON_DB")
NEON_USER = os.getenv("NEON_USER")
NEON_PASSWORD = os.getenv("NEON_PASSWORD")

# Iceberg Configuration
SCHEMA_NAME = "HMD"
BRONZE_BUCKET_PATH = "s3a://hmd-lakehouse/bronze"
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "glacier_iceberg")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://hmd-lakehouse/iceberg/warehouse")

def reconcile_counts(date_str, bronze_count, iceberg_count):
    """Trace shows row count reconciliation with compute."""
    is_balanced = bronze_count == iceberg_count
    status_icon = "✅ MATCH" if is_balanced else "❌ MISMATCH"
    matched = "All rows computed to Iceberg storage" if is_balanced else "Row count failed – check compute partial result"
    print(f"\n   🔍 Reconciliation [{date_str}]: completed_rows={bronze_count} -> {iceberg_count}  {status_icon}")
    if not is_balanced:
        diff = bronze_count - iceberg_count
        print(f"      ⚠️  {diff} row(s) unaccounted for from completed results — check partial compute")
    return is_balanced


# 1. Spark Initialization with Iceberg Support

spark = (
    SparkSession.builder.appName("HMD_Silver_Iceberg_Ingestion")
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
    .config("spark.hadoop.parquet.block.size", "33554432")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{SCHEMA_NAME}")
print("✅ Spark Session Initialized for Silver Iceberg Layer Ingestion.")

print("\n" + "=" * 60)
print("ICEBERG SILVER LAYER INGESTION - STARTING")
print("=" * 60)


# 2. Reusable Data-Quality Gateway

def apply_dq_gateway(df, table_name, pk_col, rules):
    """Apply data quality checks to raw DataFrame."""
    null_pk_cond = F.col(pk_col).isNull()

    w = Window.partitionBy(pk_col).orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w))
    dup_cond = (F.col("_rn") > 1) & F.col(pk_col).isNotNull()

    all_rules = [("null_primary_key", null_pk_cond),
                 ("duplicate_primary_key", dup_cond)] + rules

    reason_cols = []
    for reason, cond in all_rules:
        rc = f"_fail_{reason}"
        df = df.withColumn(rc, F.when(cond, F.lit(reason)))
        reason_cols.append(rc)

    df = df.withColumn(
        "_dq_failures",
        F.filter(F.array(*[F.col(c) for c in reason_cols]), lambda x: x.isNotNull()),
    )
    df = df.drop(*reason_cols, "_rn")

    valid_df = df.filter(F.size("_dq_failures") == 0).drop("_dq_failures")

    quarantine_df = (
        df.filter(F.size("_dq_failures") > 0)
        .withColumn("_dq_failure_reason", F.concat_ws("; ", F.col("_dq_failures")))
        .drop("_dq_failures")
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_source_table", F.lit(table_name))
    )
    return valid_df, quarantine_df


def check_referential_integrity(df, fk_col, parent_valid_df, parent_key_col, table_name, reason_label):
    """Anti-join style FK check."""
    parent_keys = parent_valid_df.select(F.col(parent_key_col).alias("_parent_key")).distinct()
    joined = df.join(parent_keys, df[fk_col] == F.col("_parent_key"), "left")

    valid = joined.filter(F.col("_parent_key").isNotNull()).drop("_parent_key")
    invalid = (
        joined.filter(F.col("_parent_key").isNull())
        .drop("_parent_key")
        .withColumn("_dq_failure_reason", F.lit(reason_label))
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_source_table", F.lit(table_name))
    )
    return valid, invalid


def write_quarantine(quarantine_dfs, table_name):
    """Union and persist quarantine records (overwrite per run)."""
    combined = None
    for qdf in quarantine_dfs:
        if qdf is None:
            continue
        combined = qdf if combined is None else combined.unionByName(qdf, allowMissingColumns=True)

    if combined is None:
        print(f"   ⚠️  No quarantine DataFrame provided for {table_name} — nothing written.")
        return 0

    combined = combined.cache()
    q_count = combined.count()
    target_path = f"s3a://hmd-lakehouse/quarantine/failed_records/{table_name}"
    combined.coalesce(4).write.mode("overwrite").parquet(target_path)

    if q_count > 0:
        print(f"   🚧 {q_count} record(s) quarantined -> {target_path}")
    else:
        print(f"   ✅ No records quarantined for {table_name} (cleared any stale quarantine).")
    return q_count


# 3. Data Processing Functions

def process_patients():
    print("\n🔄 Processing Silver: Patients (Iceberg)...")

    df = spark.read.parquet(f"{BRONZE_BUCKET_PATH}/patients")
    bronze_count = df.count()
    print(f"   📊 Bronze row count: {bronze_count}")

    df_clean = (
        df.withColumn("patient_id", F.trim(F.col("patient_id")))
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        .withColumn("gender", F.upper(F.trim(F.col("gender"))))
        .withColumn("date_of_birth", F.to_date(F.col("date_of_birth")))
        .withColumn("registration_date", F.to_date(F.col("registration_date")))
        .withColumn("email", F.trim(F.col("email")))
        .withColumn(
            "age",
            F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25).cast(IntegerType()),
        )
    )

    rules = [
        ("invalid_date_of_birth",
         F.col("date_of_birth").isNull() | (F.col("date_of_birth") > F.current_date())),
        ("age_out_of_range",
         F.col("age").isNull() | (F.col("age") < 0) | (F.col("age") > 120)),
        ("invalid_gender", ~F.col("gender").isin("M", "F")),
    ]
    valid_df, dq_quarantine = apply_dq_gateway(df_clean, "patients", "patient_id", rules)

    valid_df = valid_df.cache()
    quarantine_count = write_quarantine([dq_quarantine], "patients")

    valid_df.writeTo(f"{ICEBERG_CATALOG}.{SCHEMA_NAME}.patients").using("iceberg").createOrReplace()

    valid_count = valid_df.count()
    print(f"✅ Patients (Iceberg): {valid_count} valid rows written to {ICEBERG_CATALOG}.{SCHEMA_NAME}.patients")

    # Row count reconciliation after write
    try:
        iceberg_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {ICEBERG_CATALOG}.{SCHEMA_NAME}.patients").collect()[0]["cnt"]
        reconcile_counts("patients", bronze_count, iceberg_count + quarantine_count)
    except Exception as e:
        print(f"   ⚠️  Failed to read back Iceberg row count: {e}")
        reconcile_counts("patients", bronze_count, valid_count + quarantine_count)

    return valid_df


def process_doctors():
    print("\n🔄 Processing Silver: Doctors (Iceberg)...")

    df = spark.read.parquet(f"{BRONZE_BUCKET_PATH}/doctors")
    bronze_count = df.count()
    print(f"   📊 Bronze row count: {bronze_count}")

    df_clean = (
        df.withColumn("doctor_id", F.trim(F.col("doctor_id")))
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        .withColumn("specialization", F.initcap(F.trim(F.col("specialization"))))
        .withColumn("hospital_branch", F.trim(F.col("hospital_branch")))
        .withColumn("years_experience", F.col("years_experience").cast(IntegerType()))
    )

    rules = [
        ("invalid_years_experience",
         F.col("years_experience").isNull() | (F.col("years_experience") < 0)),
    ]
    valid_df, dq_quarantine = apply_dq_gateway(df_clean, "doctors", "doctor_id", rules)

    valid_df = valid_df.cache()
    quarantine_count = write_quarantine([dq_quarantine], "doctors")

    valid_df.writeTo(f"{ICEBERG_CATALOG}.{SCHEMA_NAME}.doctors").using("iceberg").createOrReplace()

    valid_count = valid_df.count()
    print(f"✅ Doctors (Iceberg): {valid_count} valid rows written to {ICEBERG_CATALOG}.{SCHEMA_NAME}.doctors")
    reconcile_counts("doctors", bronze_count, valid_count + quarantine_count)

    return valid_df


def process_appointments(valid_patients_df, valid_doctors_df):
    print("\n🔄 Processing Silver: Appointments (Iceberg)...")

    df = spark.read.parquet(f"{BRONZE_BUCKET_PATH}/appointments")
    bronze_count = df.count()
    print(f"   📊 Bronze row count: {bronze_count}")

    padded_time = F.regexp_replace(F.trim(F.col("appointment_time")), r"^(\d):", r"0$1:")

    status_lower = F.lower(F.trim(F.col("status")))
    status_clean = (
        F.when(status_lower.isin("no-show", "no show", "noshow"), F.lit("No-Show"))
        .when(status_lower == "scheduled", F.lit("Scheduled"))
        .when(status_lower == "completed", F.lit("Completed"))
        .when(status_lower == "cancelled", F.lit("Cancelled"))
        .otherwise(F.initcap(F.trim(F.col("status"))))
    )

    df_clean = (
        df.withColumn("appointment_id", F.trim(F.col("appointment_id")))
        .withColumn("patient_id", F.trim(F.col("patient_id")))
        .withColumn("doctor_id", F.trim(F.col("doctor_id")))
        .withColumn("status", status_clean)
        .withColumn("appointment_date", F.to_date(F.col("appointment_date")))
        .withColumn(
            "appointment_timestamp",
            F.to_timestamp(
                F.concat_ws(" ", F.col("appointment_date"), padded_time), "yyyy-MM-dd HH:mm:ss"
            ),
        )

        .withColumn("is_noshow", F.col("status") == "No-Show")
    )

    rules = [
        ("invalid_status_enum",
         ~F.col("status").isin("Scheduled", "Completed", "Cancelled", "No-Show")),

        ("future_appointment_date",
         F.col("appointment_date").isNull() | (F.col("appointment_date") > F.current_date())),
    ]
    valid_df, dq_quarantine = apply_dq_gateway(df_clean, "appointments", "appointment_id", rules)

    # Referential integrity: every appointment must point at a real patient/doctor.
    valid_df, fk_patient_q = check_referential_integrity(
        valid_df, "patient_id", valid_patients_df, "patient_id", "appointments", "orphaned_patient_id"
    )
    valid_df, fk_doctor_q = check_referential_integrity(
        valid_df, "doctor_id", valid_doctors_df, "doctor_id", "appointments", "orphaned_doctor_id"
    )

    valid_df = valid_df.cache()
    quarantine_count = write_quarantine([dq_quarantine, fk_patient_q, fk_doctor_q], "appointments")

    # Create or replace Iceberg table
    valid_df.writeTo(f"{ICEBERG_CATALOG}.{SCHEMA_NAME}.appointments").using("iceberg").createOrReplace()

    valid_count = valid_df.count()
    print(f"✅ Appointments (Iceberg): {valid_count} valid rows written to {ICEBERG_CATALOG}.{SCHEMA_NAME}.appointments")
    reconcile_counts("appointments", bronze_count, valid_count + quarantine_count)

    return valid_df


def process_treatments(valid_appointments_df):
    print("\n🔄 Processing Silver: Treatments (Iceberg)...")

    df = spark.read.parquet(f"{BRONZE_BUCKET_PATH}/treatments")
    bronze_count = df.count()
    print(f"   📊 Bronze row count: {bronze_count}")

    df_clean = (
        df.withColumn("treatment_id", F.trim(F.col("treatment_id")))
        .withColumn("appointment_id", F.trim(F.col("appointment_id")))
        .withColumn("treatment_type", F.initcap(F.trim(F.col("treatment_type"))))
        .withColumn("cost", F.regexp_replace(F.col("cost"), '"', "").cast(DecimalType(10, 2)))
        .withColumn("treatment_date", F.to_timestamp(F.col("treatment_date")))
        # Highlight values that are zero vs standard positive costs
        .withColumn(
            "treatment_cost_status",
            F.when(F.col("cost") == 0, F.lit("FREE_OR_COVERED"))
            .when(F.col("cost") > 0, F.lit("STANDARD_COST"))
            .otherwise(F.lit("UNKNOWN"))
        )
    )

    # Only quarantine STRICTLY negative (< 0) or Null costs
    rules = [
        ("invalid_negative_or_null_cost",
         F.col("cost").isNull() | (F.col("cost") < 0)),
    ]
    valid_df, dq_quarantine = apply_dq_gateway(df_clean, "treatments", "treatment_id", rules)

    valid_df, fk_appt_q = check_referential_integrity(
        valid_df, "appointment_id", valid_appointments_df, "appointment_id",
        "treatments", "orphaned_appointment_id"
    )

    valid_df = valid_df.cache()
    quarantine_count = write_quarantine([dq_quarantine, fk_appt_q], "treatments")

    valid_df.writeTo(f"{ICEBERG_CATALOG}.{SCHEMA_NAME}.treatments").using("iceberg").createOrReplace()

    valid_count = valid_df.count()
    print(f"✅ Treatments (Iceberg): {valid_count} valid rows written to {ICEBERG_CATALOG}.{SCHEMA_NAME}.treatments")
    reconcile_counts("treatments", bronze_count, valid_count + quarantine_count)

    return valid_df


def process_billing(valid_patients_df, valid_treatments_df):
    print("\n🔄 Processing Silver: Billing (Iceberg)...")

    df = spark.read.parquet(f"{BRONZE_BUCKET_PATH}/billing")
    bronze_count = df.count()
    print(f"   📊 Bronze row count: {bronze_count}")

    # Checking insurance presence if column exists, else standard check
    has_insurance_col = "insurance_provider" in df.columns

    if has_insurance_col:
        insurance_cond = F.col("insurance_provider").isNotNull() & (F.trim(F.col("insurance_provider")) != "")
    else:
        insurance_cond = F.lit(False)

    df_clean = (
        df.withColumn("bill_id", F.trim(F.col("bill_id")))
        .withColumn("patient_id", F.trim(F.col("patient_id")))
        .withColumn("treatment_id", F.trim(F.col("treatment_id")))
        .withColumn("amount", F.regexp_replace(F.col("amount"), '"', "").cast(DecimalType(10, 2)))
        .withColumn("payment_method", F.initcap(F.trim(F.col("payment_method"))))
        .withColumn("payment_status", F.initcap(F.trim(F.col("payment_status"))))
        .withColumn("bill_date", F.to_timestamp(F.col("bill_date")))
        # Highlight zero amounts based on Insurance context
        .withColumn(
            "billing_status",
            F.when((F.col("amount") == 0) & insurance_cond, F.lit("COVERED_BY_INSURANCE"))
            .when((F.col("amount") == 0) & ~insurance_cond, F.lit("ZERO_AMOUNT_NO_INSURANCE"))
            .when(F.col("amount") > 0, F.lit("STANDARD_BILL"))
            .otherwise(F.lit("UNKNOWN"))
        )
    )

    # Only quarantine STRICTLY negative (< 0) or Null amounts
    rules = [
        ("invalid_negative_or_null_amount",
         F.col("amount").isNull() | (F.col("amount") < 0)),
        ("invalid_payment_status_enum",
         ~F.col("payment_status").isin("Paid", "Pending", "Failed", "Refunded")),
    ]
    valid_df, dq_quarantine = apply_dq_gateway(df_clean, "billing", "bill_id", rules)

    valid_df, fk_patient_q = check_referential_integrity(
        valid_df, "patient_id", valid_patients_df, "patient_id", "billing", "orphaned_patient_id"
    )
    valid_df, fk_treatment_q = check_referential_integrity(
        valid_df, "treatment_id", valid_treatments_df, "treatment_id", "billing", "orphaned_treatment_id"
    )

    valid_df = valid_df.cache()
    quarantine_count = write_quarantine([dq_quarantine, fk_patient_q, fk_treatment_q], "billing")

    valid_df.writeTo(f"{ICEBERG_CATALOG}.{SCHEMA_NAME}.billing").using("iceberg").createOrReplace()

    valid_count = valid_df.count()
    print(f"✅ Billing (Iceberg): {valid_count} valid rows written to {ICEBERG_CATALOG}.{SCHEMA_NAME}.billing")
    reconcile_counts("billing", bronze_count, valid_count + quarantine_count)

    return valid_df


# 4. Execution Pipeline

if __name__ == "__main__":
    try:
        print("\n" + "=" * 60)
        print("🏗️  SILVER LAYER ICEBERG INTEGRATION PIPELINE")
        print("=" * 60)
        print(f"DATABASE: Neon ({NEON_DB})")
        print(f"ICEBERG CATALOG: {ICEBERG_CATALOG}")
        print(f"WAREHOUSE: {ICEBERG_WAREHOUSE}")
        print("=" * 60)

        patients_silver = process_patients()
        doctors_silver = process_doctors()
        appointments_silver = process_appointments(patients_silver, doctors_silver)
        treatments_silver = process_treatments(appointments_silver)
        billing_silver = process_billing(patients_silver, treatments_silver)

        print("\n" + "=" * 60)
        print("RECONCILIATION SUMMARY")
        print("=" * 60)
        # (Reconciliation results summary omitted for brevity)
        print("\n🎉 SILVER LAYER ICEBERG PIPELINE COMPLETED SUCCESSFULLY! ✨")

    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        raise
    finally:
        spark.stop()