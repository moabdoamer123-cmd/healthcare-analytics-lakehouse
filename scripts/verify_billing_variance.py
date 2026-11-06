#!/usr/bin/env python3
"""
Verify Billing Variance: Does billed_amount always equal treatment_cost?
Panoras Na show real data proves assumptions in README are WRONG or CORRECT.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

spark = SparkSession.builder \
    .appName("Verify_Billing_Variance") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

BRONZE_BASE_PATH = "s3a://hmd-lakehouse/bronze"
SILVER_BASE_PATH = "s3a://hmd-lakehouse/silver"

print("\n🔍 VERIFYING BILLING VARIANCE IN ACTUAL DATA")
print("=" * 80 + "\n")

try:
    print(f"Reading from {BRONZE_BASE_PATH}...")
    df_billing = spark.read.parquet(f"{BRONZE_BASE_PATH}/billing")
    df_treatments = spark.read.parquet(f"{BRONZE_BASE_PATH}/treatments")
    source = "Bronze Layer (Source Data)"
except Exception as e:
    print(f"Bronze not accessible: {e}")
    print("Falling back to Silver...")
    df_billing = spark.read.parquet(f"{SILVER_BASE_PATH}/billing")
    df_treatments = spark.read.parquet(f"{SILVER_BASE_PATH}/treatments")
    source = "Silver Layer (Fallback)"

print(f"   Bronze billing records: {df_billing.count():,}")
print(f"   Bronze treatments records: {df_treatments.count():,}\n")

# Inner join to match billing-treatment pairs
df_joined = df_billing.alias("bil").join(
    df_treatments.alias("trt"),
    df_billing["treatment_id"] == df_treatments["treatment_id"],
    "inner"
)

if df_joined.count() == 0:
    print("❌ No matching pairs found!")
    sys.exit(1)

print(f"   Matching pairs: {df_joined.count():,}\n")

analysis = df_joined.select(
    F.col("bil.amount").alias("billed"),
    F.col("trt.cost").alias("cost"),
    (F.col("bil.amount") - F.col("trt.cost")).alias("variance")
)

zero_count = analysis.filter(F.col("variance") == 0).count()
total_count = analysis.count()
zero_pct = (zero_count / total_count) * 100

print("=" * 80)
print(f"📊 VARIANCE DISTRIBUTION (FROM {source})")
print("=" * 80)
print(f"✅ Zero variance:       {zero_count:,} ({zero_pct:.2f}%)")
print(f"➕ Positive variance:   {analysis.filter(F.col('variance') > 0).count():,}")
print(f"➖ Negative variance:   {analysis.filter(F.col('variance') < 0).count():,}\n")

# Show samples
print("=" * 80)
print("🔍 SAMPLE: Zero-Variance Records")
print("=" * 80)
df_joined.filter(
    (F.col("bil.amount") - F.col("trt.cost")) == 0
).limit(3).show(truncate=False)

print("\n" + "=" * 80)
print("🎯 VERDICT")
print("=" * 80 + "\n")

if zero_pct > 95:
    print("❌ CRITICAL: >95% ZERO VARIANCE")
    print(f"   This is in {source} BEFORE any Gold transformation!")
    print("\nConclusion: This is NOT a transformation bug.")
    print("The source data itself has billing == cost. Something in the data generation:")
    print("  - Might be demo/sample data with fixed prices")
    print("  - ETL feeding Neon might already be consolidating/subsidizing")
    print("  - Or the raw JSON/CSV source had bug where cost was hard-coded = billed")
else:
    print(f"✅ OK: Only {zero_pct:.2f}% zero variance")
    print("   Data quality is acceptable - real business variance exists")

print("\n" + "=" * 80 + "\n")
spark.stop()