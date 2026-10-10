import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# 1. Spark Session Configuration

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

BRONZE_BASE = "s3a://hmd-lakehouse/bronze"
SILVER_BASE = "s3a://hmd-lakehouse/silver"
GOLD_BASE = "s3a://hmd-lakehouse/gold"
QUARANTINE_BASE = "s3a://hmd-lakehouse/quarantine/failed_records"

PK_COLUMNS = {
    "patients": "patient_id",
    "doctors": "doctor_id",
    "appointments": "appointment_id",
    "treatments": "treatment_id",
    "billing": "bill_id",
}

CHECKS_FAILED = []


def create_spark_session():
    return (
        SparkSession.builder.appName("HMD_End_To_End_Reconciliation")
        # FIX: these were missing entirely — without them every s3a:// read
        # throws "No FileSystem for scheme s3a" before any check can run.
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
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def safe_read_parquet(spark, path):

    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def check(label, passed, detail):
    icon = "✅" if passed else "❌"
    print(f"{icon} {label}: {detail}")
    if not passed:
        CHECKS_FAILED.append(label)


# 2. Bronze -> Silver checks (quarantine expected)

def row_count_check(spark, table_name):

    bronze_count = spark.read.parquet(f"{BRONZE_BASE}/{table_name}").count()

    silver_count = spark.read.parquet(f"{SILVER_BASE}/{table_name}").count()

    quarantine_df = safe_read_parquet(spark, f"{QUARANTINE_BASE}/{table_name}")
    quarantine_count = quarantine_df.count() if quarantine_df is not None else 0

    accounted_for = silver_count + quarantine_count
    check(
        f"Row count [{table_name}]",
        accounted_for == bronze_count,
        f"bronze={bronze_count}  vs  silver={silver_count} + quarantine={quarantine_count} = {accounted_for}",
    )
    return silver_count


def financial_sum_check(spark, table_name, amount_col):
    """RAW bronze (cleaned) sum must equal silver sum + quarantine sum, within a cent. Same fix as row_count_check — no pre-dedup."""
    bronze_df = spark.read.parquet(f"{BRONZE_BASE}/{table_name}")
    bronze_clean = bronze_df.withColumn(
        amount_col, F.regexp_replace(F.col(amount_col), '"', "").cast(DecimalType(10, 2))
    )
    bronze_total = bronze_clean.select(F.sum(amount_col)).collect()[0][0] or 0

    silver_total = spark.read.parquet(f"{SILVER_BASE}/{table_name}").select(F.sum(amount_col)).collect()[0][0] or 0

    quarantine_df = safe_read_parquet(spark, f"{QUARANTINE_BASE}/{table_name}")
    if quarantine_df is not None:
  
        quarantine_total = (
            quarantine_df.withColumn(
                amount_col, F.regexp_replace(F.col(amount_col).cast("string"), '"', "").cast(DecimalType(10, 2))
            )
            .select(F.sum(amount_col))
            .collect()[0][0]
            or 0
        )
    else:
        quarantine_total = 0

    diff = abs(float(bronze_total) - float(silver_total) - float(quarantine_total))
    check(
        f"Financial sum [{table_name}.{amount_col}]",
        diff <= 0.01,
        f"bronze={bronze_total}  vs  silver={silver_total} + quarantine={quarantine_total}, diff={diff:.2f}",
    )


# 3. Silver -> Gold checks (lossless join, zero tolerance)

def silver_to_gold_row_count_check(spark, silver_table, gold_table, silver_count=None):
    """
    No quarantine step exists between Silver and Gold — Gold is built from
    Silver via a join Silver already guaranteed to be referentially
    intact — so row counts must match EXACTLY, not just "within reason".
    """
    if silver_count is None:
        silver_count = spark.read.parquet(f"{SILVER_BASE}/{silver_table}").count()
    gold_count = spark.read.parquet(f"{GOLD_BASE}/{gold_table}").count()
    check(
        f"Row count [Silver.{silver_table} -> Gold.{gold_table}]",
        gold_count == silver_count,
        f"silver={silver_count}  vs  gold={gold_count}",
    )


def silver_to_gold_financial_check(spark, silver_table, silver_col, gold_table, gold_col):
    silver_total = (
        spark.read.parquet(f"{SILVER_BASE}/{silver_table}").select(F.sum(silver_col)).collect()[0][0] or 0
    )
    gold_total = spark.read.parquet(f"{GOLD_BASE}/{gold_table}").select(F.sum(gold_col)).collect()[0][0] or 0
    diff = abs(float(silver_total) - float(gold_total))
    check(
        f"Financial sum [Silver.{silver_table}.{silver_col} -> Gold.{gold_table}.{gold_col}]",
        diff <= 0.01,
        f"silver={silver_total}  vs  gold={gold_total}, diff={diff:.2f}",
    )


def revenue_variance_sanity_check(spark):
    """
    Bonus assertion beyond what Requirement 4 literally asks for: verifies
    the revenue_variance FORMULA itself, not just data completeness.
    SUM(revenue_variance) must equal SUM(billed_amount) - SUM(treatment_cost)
    computed independently — catches a bug in the Gold script's own
    calculation, which a pure row-count/sum check wouldn't detect.
    """
    fact = spark.read.parquet(f"{GOLD_BASE}/fact_billing_treatments")
    totals = fact.select(
        F.sum("billed_amount").alias("total_billed"),
        F.sum("treatment_cost").alias("total_cost"),
        F.sum("revenue_variance").alias("total_variance"),
    ).collect()[0]

    expected_variance = float(totals["total_billed"] or 0) - float(totals["total_cost"] or 0)
    actual_variance = float(totals["total_variance"] or 0)
    diff = abs(expected_variance - actual_variance)
    check(
        "Revenue variance formula sanity",
        diff <= 0.01,
        f"expected(billed-cost)={expected_variance:.2f}  vs  actual sum(revenue_variance)={actual_variance:.2f}",
    )



# 4. Execution

def run_end_to_end_reconciliation():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("\n🔍 Running End-to-End Row-Count & Financial Reconciliation (Requirement 4, Task 5)...\n")

    print("--- Stage 1: Bronze -> Silver (row counts) ---")
    silver_counts = {t: row_count_check(spark, t) for t in PK_COLUMNS}

    print("\n--- Stage 1: Bronze -> Silver (financial sums) ---")

    financial_sum_check(spark, "treatments", "cost")
    financial_sum_check(spark, "billing", "amount")

    print("\n--- Stage 2: Silver -> Gold (row counts) ---")
    silver_to_gold_row_count_check(spark, "patients", "dim_patients", silver_counts["patients"])
    silver_to_gold_row_count_check(spark, "doctors", "dim_doctors", silver_counts["doctors"])
    silver_to_gold_row_count_check(spark, "treatments", "dim_treatments", silver_counts["treatments"])
    silver_to_gold_row_count_check(spark, "appointments", "fact_appointments", silver_counts["appointments"])
    silver_to_gold_row_count_check(spark, "billing", "fact_billing_treatments", silver_counts["billing"])

    print("\n--- Stage 2: Silver -> Gold (financial sums) ---")

    silver_to_gold_financial_check(spark, "treatments", "cost", "dim_treatments", "cost")
    silver_to_gold_financial_check(spark, "billing", "amount", "fact_billing_treatments", "billed_amount")

    print("\n--- Bonus: Gold formula sanity ---")
    revenue_variance_sanity_check(spark)

    print("\n" + "=" * 60)
    if CHECKS_FAILED:
        print(f"❌ RECONCILIATION FAILED — {len(CHECKS_FAILED)} check(s) did not pass:")
        for c in CHECKS_FAILED:
            print(f"   - {c}")
        print("=" * 60)
        spark.stop()

        raise ValueError("End-to-end reconciliation failed — see failed checks above.")

    print("✅ END-TO-END RECONCILIATION CHECKS COMPLETED SUCCESSFULLY — zero silent data loss confirmed.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    run_end_to_end_reconciliation()