import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, year, month, dayofmonth, col

# ==========================================
# 1. Configuration Credentials
# ==========================================
# Pull from environment variables when available; the fallback values are
# only for local convenience. Rotate the Neon password and move these to a
# .env file / real env vars before this project goes anywhere near git.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

NEON_HOST = os.getenv(
    "NEON_HOST", "ep-tiny-sunset-ah3xbklk-pooler.c-3.us-east-1.aws.neon.tech"
)
NEON_PORT = os.getenv("NEON_PORT", "5432")
NEON_DB = os.getenv("NEON_DB", "neondb")
NEON_USER = os.getenv("NEON_USER", "neondb_owner")
NEON_PASSWORD = os.getenv("NEON_PASSWORD", "npg_nf0Bpoz8SKtF")

# Schema in Neon is literally "HMD" (uppercase, quoted at creation time) —
# always quote it in raw SQL so Postgres matches it exactly.
SCHEMA_NAME = "HMD"

# FIX #1: bucket name must match Requirement 1 exactly -> hmd-lakehouse
BRONZE_BUCKET_PATH = "s3a://hmd-lakehouse/bronze"

# ==========================================
# 2. Initialize Spark Session
# ==========================================
spark = (
    SparkSession.builder.appName("HMD_Bronze_Ingestion")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.6.0",
    )
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

print("🚀 Starting Bronze Layer Ingestion Pipeline...")

jdbc_url = f"jdbc:postgresql://{NEON_HOST}:{NEON_PORT}/{NEON_DB}?sslmode=require"

tables = ["patients", "doctors", "appointments", "treatments", "billing"]
results = {}

# ==========================================
# 3. Extract & Write to MinIO Bronze Bucket
# ==========================================
for table in tables:
    print(f"\n📥 Extracting \"{SCHEMA_NAME}\".{table} from Neon Postgres...")
    try:
        # Read raw table from Neon
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f'(SELECT * FROM "{SCHEMA_NAME}".{table}) AS t')
            .option("user", NEON_USER)
            .option("password", NEON_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # FIX #2: real partition columns (not just a timestamp value inside
        # the rows). These become actual folder levels on write, e.g.
        # bronze/patients/ingestion_year=2026/ingestion_month=08/ingestion_day=25/
        ingestion_ts_col = current_timestamp()
        bronze_df = (
            df.withColumn("_ingested_at", ingestion_ts_col)
            .withColumn("_source_table", lit(table))
            .withColumn("ingestion_year", year(col("_ingested_at")))
            .withColumn("ingestion_month", month(col("_ingested_at")))
            .withColumn("ingestion_day", dayofmonth(col("_ingested_at")))
        )

        target_path = f"{BRONZE_BUCKET_PATH}/{table}"
        print(f"💾 Writing raw parquet data to -> {target_path} (partitioned)...")

        # FIX #3: append, not overwrite. Bronze must preserve full ingestion
        # audit history — overwrite would destroy every prior run's data.
        # Because we partition by ingestion_year/month/day, each run lands
        # in its own new partition folder, so append never duplicates or
        # clobbers a previous day's data; re-running on the SAME day will
        # add to that day's partition, which is expected for re-runs.
        (
            bronze_df.write.format("parquet")
            .mode("append")
            .partitionBy("ingestion_year", "ingestion_month", "ingestion_day")
            .save(target_path)
        )

        row_count = bronze_df.count()
        print(f"✅ Successfully ingested {row_count} records into {target_path}")
        results[table] = f"OK ({row_count} rows)"

    except Exception as e:
        # FIX #4: don't let one failing table kill the whole ingestion run —
        # log it, keep going, and report everything at the end.
        print(f"❌ Failed to ingest \"{SCHEMA_NAME}\".{table}: {e}")
        results[table] = "FAILED"

# ==========================================
# 4. Summary
# ==========================================
print("\n" + "=" * 50)
print("BRONZE INGESTION SUMMARY")
print("=" * 50)
for table, status in results.items():
    icon = "✅" if status.startswith("OK") else "❌"
    print(f"{icon} {table}: {status}")

if all(status.startswith("OK") for status in results.values()):
    print("\n🎉 BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY!")
else:
    print("\n⚠️  BRONZE LAYER INGESTION COMPLETED WITH ERRORS — check log above.")

print("=" * 50)

spark.stop()