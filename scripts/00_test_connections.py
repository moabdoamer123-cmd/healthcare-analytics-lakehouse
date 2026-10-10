import os
from pyspark.sql import SparkSession

# 1. Configuration Credentials
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

SCHEMA_NAME = "HMD"

# 2. Initialize Spark Session with S3A & JDBC
spark = (
    SparkSession.builder.appName("HMD_Test_Connections")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.6.0",
    )
    # MinIO S3A Configurations
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

print("✅ Spark Session initialized successfully!")


# 3. Test Connection to Each Table

jdbc_url = f"jdbc:postgresql://{NEON_HOST}:{NEON_PORT}/{NEON_DB}?sslmode=require"

tables = ["patients", "doctors", "appointments", "treatments", "billing"]

results = {}

for table in tables:
    print(f"\n🔄 Fetching sample data from \"{SCHEMA_NAME}\".{table}...")
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f'(SELECT * FROM "{SCHEMA_NAME}".{table}) AS t')
            .option("user", NEON_USER)
            .option("password", NEON_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        row_count = df.count()
        print(f"✅ Successfully loaded \"{SCHEMA_NAME}\".{table} — {row_count} rows")
        df.printSchema()
        df.show(3, truncate=False)
        results[table] = "OK"

    except Exception as e:
   
        print(f"❌ Failed to load \"{SCHEMA_NAME}\".{table}: {e}")
        results[table] = "FAILED"


# 4. Summary

print("\n" + "=" * 50)
print("CONNECTION TEST SUMMARY")
print("=" * 50)
for table, status in results.items():
    icon = "✅" if status == "OK" else "❌"
    print(f"{icon} {table}: {status}")

spark.stop()