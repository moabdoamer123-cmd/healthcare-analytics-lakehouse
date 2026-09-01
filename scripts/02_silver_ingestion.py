import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, IntegerType

# ==========================================
# 1. Spark Session Configuration
# ==========================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

spark = (
    SparkSession.builder.appName("HMD_Silver_Ingestion")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .config("spark.driver.extraJavaOptions", "-XX:MaxDirectMemorySize=4g")
    .config("spark.executor.extraJavaOptions", "-XX:MaxDirectMemorySize=4g")
    .config("spark.hadoop.fs.s3a.multipart.size", "33554432")  # 32MB
    .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.parquet.block.size", "33554432")
    .config("spark.local.dir", "./tmp")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session Initialized for Silver Layer Ingestion.")

BRONZE_BASE_PATH = "s3a://hmd-lakehouse/bronze"
SILVER_BASE_PATH = "s3a://hmd-lakehouse/silver"
# FIX: this path now matches Requirement 3 exactly, and — unlike the
# previous version — is actually written to. Each table gets its own
# subfolder (quarantine/failed_records/<table>/) so schemas never clash,
# the same way bronze/ and silver/ are already organized per table.
QUARANTINE_BASE_PATH = "s3a://hmd-lakehouse/quarantine/failed_records"

# Collects one entry per table so we can print a full summary at the end
# and fail the whole run if any table doesn't reconcile.
RECONCILIATION_RESULTS = []


def reconcile_counts(table_name, bronze_count, silver_count, quarantine_count):
    """
    Requirement 4, Task 5: 'Reconciliation Checks — run row-count ...
    reconciliation to ensure zero silent data loss.'

    Every row read from Bronze must end up in EXACTLY one place: Silver
    (valid) or Quarantine (rejected). apply_dq_gateway and
    check_referential_integrity are both built as exhaustive, mutually
    exclusive splits (every row satisfies either the pass or the fail
    condition, never both/neither), so bronze_count should always equal
    silver_count + quarantine_count exactly. If it doesn't, something in
    the pipeline is dropping rows silently — this check exists to catch
    that immediately instead of finding out from a wrong KPI in Power BI.
    """
    accounted_for = silver_count + quarantine_count
    is_balanced = accounted_for == bronze_count
    status = "✅ MATCH" if is_balanced else "❌ MISMATCH"

    print(
        f"   🔍 Reconciliation [{table_name}]: bronze={bronze_count} -> "
        f"silver={silver_count} + quarantine={quarantine_count} = {accounted_for}  {status}"
    )
    if not is_balanced:
        missing = bronze_count - accounted_for
        print(f"      ⚠️  {missing} row(s) unaccounted for in '{table_name}' — investigate immediately!")

    RECONCILIATION_RESULTS.append(
        {
            "table": table_name,
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "quarantine_count": quarantine_count,
            "balanced": is_balanced,
        }
    )
    return is_balanced


# ==========================================
# 2. Reusable Data-Quality Gateway
# ==========================================
def apply_dq_gateway(df, table_name, pk_col, rules):
    """
    Central DQ gate applied to every table before it's allowed into Silver.

    Automatically enforces the "Uniqueness" business rule (PK must be
    non-null AND unique) on top of whatever custom `rules` you pass in:
      - null_primary_key         -> pk_col is null
      - duplicate_primary_key    -> pk_col repeats; since Bronze is
                                     append-only, we always keep the row
                                     with the latest `_ingested_at` and
                                     quarantine the older duplicate(s)
                                     instead of picking one arbitrarily.

    `rules` is a list of (reason_code, failure_condition) tuples.
    IMPORTANT: failure_condition is True when the row should be
    quarantined (i.e. it's a FAIL condition, not a pass condition).

    Returns (valid_df, quarantine_df). quarantine_df keeps every original
    column plus _dq_failure_reason / _quarantined_at / _source_table so
    every rejection is fully traceable — that's the "error metadata and
    reason logs" Requirement 3 asks for.
    """
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

    # Using array + filter(isNotNull) instead of array_remove(arr, None):
    # array_remove needs a typed literal to remove, and a bare Python None
    # doesn't resolve cleanly against a StringType array in Spark SQL.
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
    """
    Anti-join style FK check. Rows whose fk_col has no match among the
    parent's already-validated keys are pulled out and quarantined with a
    clear reason, instead of silently vanishing inside an inner join like
    the original script did.
    """
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
    """
    Union every quarantine source for this table and persist it.

    FIX: this now ALWAYS overwrites, even when there's nothing to
    quarantine this run. The previous version used append mode and simply
    skipped writing when a table had zero failures — so if an earlier
    (buggy) run had quarantined records and a later run fixed the issue,
    the old rejected rows just sat there forever, since nothing ever
    cleared them. Because Silver reprocesses the FULL Bronze history from
    scratch every run (not incrementally), quarantine should reflect only
    the CURRENT run's findings — the permanent historical record already
    lives in Bronze itself, which is append-only, so nothing is actually
    lost by not accumulating quarantine indefinitely.
    """
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
    target_path = f"{QUARANTINE_BASE_PATH}/{table_name}"
    combined.coalesce(4).write.mode("overwrite").parquet(target_path)

    if q_count > 0:
        print(f"   🚧 {q_count} record(s) quarantined -> {target_path}")
    else:
        print(f"   ✅ No records quarantined for {table_name} (cleared any stale quarantine data from earlier runs).")
    return q_count


# ==========================================
# 3. Cleaning & Validation Pipeline Functions
# ==========================================

def process_patients():
    print("\n🔄 Processing Silver: Patients...")
    df = spark.read.parquet(f"{BRONZE_BASE_PATH}/patients")
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
    valid_df.write.mode("overwrite").parquet(f"{SILVER_BASE_PATH}/patients")
    valid_count = valid_df.count()
    print(f"✅ Patients: {valid_count} valid rows written to Silver.")
    reconcile_counts("patients", bronze_count, valid_count, quarantine_count)
    return valid_df


def process_doctors():
    print("\n🔄 Processing Silver: Doctors...")
    df = spark.read.parquet(f"{BRONZE_BASE_PATH}/doctors")
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
    valid_df.write.mode("overwrite").parquet(f"{SILVER_BASE_PATH}/doctors")
    valid_count = valid_df.count()
    print(f"✅ Doctors: {valid_count} valid rows written to Silver.")
    reconcile_counts("doctors", bronze_count, valid_count, quarantine_count)
    return valid_df


def process_appointments(valid_patients_df, valid_doctors_df):
    print("\n🔄 Processing Silver: Appointments...")
    df = spark.read.parquet(f"{BRONZE_BASE_PATH}/appointments")
    bronze_count = df.count()
    print(f"   📊 Bronze row count: {bronze_count}")

    # FIX: zero-pad single-digit hours ("8:00:00" -> "08:00:00") so the
    # timestamp parser below doesn't silently return null for morning
    # appointments that happened to be stored without a leading zero.
    padded_time = F.regexp_replace(F.trim(F.col("appointment_time")), r"^(\d):", r"0$1:")

    # Normalize status to a fixed set of canonical labels up front, so the
    # domain-enum rule and the is_noshow flag both agree on the same
    # spelling regardless of how the source data was cased.
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
        # FIX: Requirement 2 explicitly asks for this — it was missing entirely.
        .withColumn("is_noshow", F.col("status") == "No-Show")
    )

    rules = [
        ("invalid_status_enum",
         ~F.col("status").isin("Scheduled", "Completed", "Cancelled", "No-Show")),
        # FIX: Requirement 3 "Date Sanity" rule — was not implemented before.
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
    valid_df.write.mode("overwrite").parquet(f"{SILVER_BASE_PATH}/appointments")
    valid_count = valid_df.count()
    print(f"✅ Appointments: {valid_count} valid rows written to Silver.")
    reconcile_counts("appointments", bronze_count, valid_count, quarantine_count)
    return valid_df


def process_treatments(valid_appointments_df):
    print("\n🔄 Processing Silver: Treatments...")
    df = spark.read.parquet(f"{BRONZE_BASE_PATH}/treatments")
    bronze_count = df.count()
    print(f"   📊 Bronze row count: {bronze_count}")

    df_clean = (
        df.withColumn("treatment_id", F.trim(F.col("treatment_id")))
        .withColumn("appointment_id", F.trim(F.col("appointment_id")))
        .withColumn("treatment_type", F.initcap(F.trim(F.col("treatment_type"))))
        .withColumn("cost", F.regexp_replace(F.col("cost"), '"', "").cast(DecimalType(10, 2)))
        .withColumn("treatment_date", F.to_timestamp(F.col("treatment_date")))
    )

    rules = [
        # FIX: rule says "strictly greater than zero (> 0)" — was ">= 0".
        # Also explicitly checks isNull(), because `null <= 0` evaluates to
        # null (not True) in Spark, so a bad cast used to sail through the
        # old `col("cost") >= 0` filter completely undetected.
        ("invalid_or_non_positive_cost",
         F.col("cost").isNull() | (F.col("cost") <= 0)),
    ]
    valid_df, dq_quarantine = apply_dq_gateway(df_clean, "treatments", "treatment_id", rules)

    valid_df, fk_appt_q = check_referential_integrity(
        valid_df, "appointment_id", valid_appointments_df, "appointment_id",
        "treatments", "orphaned_appointment_id"
    )

    valid_df = valid_df.cache()
    quarantine_count = write_quarantine([dq_quarantine, fk_appt_q], "treatments")
    valid_df.write.mode("overwrite").parquet(f"{SILVER_BASE_PATH}/treatments")
    valid_count = valid_df.count()
    print(f"✅ Treatments: {valid_count} valid rows written to Silver.")
    reconcile_counts("treatments", bronze_count, valid_count, quarantine_count)
    return valid_df


def process_billing(valid_patients_df, valid_treatments_df):
    print("\n🔄 Processing Silver: Billing...")
    df = spark.read.parquet(f"{BRONZE_BASE_PATH}/billing")
    bronze_count = df.count()
    print(f"   📊 Bronze row count: {bronze_count}")

    df_clean = (
        df.withColumn("bill_id", F.trim(F.col("bill_id")))
        .withColumn("patient_id", F.trim(F.col("patient_id")))
        .withColumn("treatment_id", F.trim(F.col("treatment_id")))
        .withColumn("amount", F.regexp_replace(F.col("amount"), '"', "").cast(DecimalType(10, 2)))
        .withColumn("payment_method", F.initcap(F.trim(F.col("payment_method"))))
        .withColumn("payment_status", F.initcap(F.trim(F.col("payment_status"))))
        .withColumn("bill_date", F.to_timestamp(F.col("bill_date")))
    )

    rules = [
        ("invalid_or_non_positive_amount",
         F.col("amount").isNull() | (F.col("amount") <= 0)),
        ("invalid_payment_status_enum",
         ~F.col("payment_status").isin("Paid", "Pending", "Failed", "Refunded")),
    ]
    valid_df, dq_quarantine = apply_dq_gateway(df_clean, "billing", "bill_id", rules)

    valid_df, fk_patient_q = check_referential_integrity(
        valid_df, "patient_id", valid_patients_df, "patient_id", "billing", "orphaned_patient_id"
    )
    # Requirement 3's explicit rule: billing.treatment_id must match a
    # real row in hmd.treatments.
    valid_df, fk_treatment_q = check_referential_integrity(
        valid_df, "treatment_id", valid_treatments_df, "treatment_id", "billing", "orphaned_treatment_id"
    )

    valid_df = valid_df.cache()
    quarantine_count = write_quarantine([dq_quarantine, fk_patient_q, fk_treatment_q], "billing")
    valid_df.write.mode("overwrite").parquet(f"{SILVER_BASE_PATH}/billing")
    valid_count = valid_df.count()
    print(f"✅ Billing: {valid_count} valid rows written to Silver.")
    reconcile_counts("billing", bronze_count, valid_count, quarantine_count)
    return valid_df


# ==========================================
# 4. Execution Pipeline
# ==========================================
if __name__ == "__main__":
    try:
        patients_silver = process_patients()
        doctors_silver = process_doctors()
        appointments_silver = process_appointments(patients_silver, doctors_silver)
        treatments_silver = process_treatments(appointments_silver)
        billing_silver = process_billing(patients_silver, treatments_silver)

        print("\n" + "=" * 50)
        print("RECONCILIATION SUMMARY (Requirement 4, Task 5)")
        print("=" * 50)
        all_balanced = True
        for r in RECONCILIATION_RESULTS:
            icon = "✅" if r["balanced"] else "❌"
            print(
                f"{icon} {r['table']:<15} bronze={r['bronze_count']:<6} "
                f"silver={r['silver_count']:<6} quarantine={r['quarantine_count']}"
            )
            all_balanced = all_balanced and r["balanced"]

        if not all_balanced:
            raise ValueError(
                "Row-count reconciliation failed for one or more tables — "
                "possible silent data loss. See the mismatch details above."
            )

        print("\n" + "=" * 50)
        print("🎉 SILVER LAYER PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 50)
    except Exception as e:
        # FIX: the original script printed the error but never re-raised
        # it, so the process exited with code 0 even on total failure.
        # In Airflow (Requirement 4) that means a broken pipeline would
        # show up as a green ✅ task. Re-raising makes the failure real.
        print(f"\n❌ Pipeline failed with error: {e}")
        raise
    finally:
        spark.stop()