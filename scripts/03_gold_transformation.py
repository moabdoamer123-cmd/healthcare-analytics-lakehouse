import os
import glob
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

NEON_HOST = os.getenv("NEON_HOST")
NEON_PORT = os.getenv("NEON_PORT")
NEON_DB = os.getenv("NEON_DB")
NEON_USER = os.getenv("NEON_USER")
NEON_PASSWORD = os.getenv("NEON_PASSWORD")
NEON_JDBC_URL = f"jdbc:postgresql://{NEON_HOST}:{NEON_PORT}/{NEON_DB}?sslmode=require"

SILVER_BASE_PATH = "s3a://hmd-lakehouse/silver"
GOLD_BASE_PATH = "s3a://hmd-lakehouse/gold"
LOCAL_EXPORT_DIR = os.getenv("GOLD_LOCAL_EXPORT_DIR", "./output")


def create_spark_session():
    return (
        SparkSession.builder.appName("HMD_Gold_Transformation")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.7.3",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.parquet.block.size", "33554432")
        .config("spark.driver.extraJavaOptions", "-XX:MaxDirectMemorySize=2g")
        .config("spark.executor.extraJavaOptions", "-XX:MaxDirectMemorySize=2g")
        .config("spark.local.dir", "./tmp")
        .getOrCreate()
    )



def write_to_neon(df, table_name):
    (df.write
       .mode("overwrite")
       .option("truncate", "true")
       .jdbc(
           NEON_JDBC_URL,
           table_name,
           properties={
               "user": NEON_USER,
               "password": NEON_PASSWORD,
               "driver": "org.postgresql.Driver",
           },
       ))
    print(f"   ✅ {table_name} written to Neon ({df.count()} rows).")



def export_single_parquet_locally(df, table_name):
    """Export one Gold table as a single local Parquet file for Power BI.

    Power BI Desktop reads ./output/<table>/<table>.parquet through its
    Python/Parquet connector. GOLD_LOCAL_EXPORT_DIR points at the container
    path that is volume-mounted back to the project folder
    (/opt/airflow/output in docker-compose.yaml), so the files land on the
    host where Power BI can see them; when the script runs directly on the
    host it defaults to ./output.
    """
    output_dir = os.path.join(LOCAL_EXPORT_DIR, table_name)
    os.makedirs(output_dir, exist_ok=True)
    for old_file in glob.glob(os.path.join(output_dir, "*")):
        if os.path.isfile(old_file):
            os.remove(old_file)
    output_file_path = os.path.join(output_dir, f"{table_name}.parquet")
    df.toPandas().to_parquet(output_file_path, engine="pyarrow", index=False)
    print(f"   \U0001F4C4 Exported {table_name} for Power BI -> {output_file_path}")


def write_gold_table(df, table_name):
    df = df.cache()
    df.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}/{table_name}")
    print(f"   ✅ {table_name} written to MinIO ({df.count()} rows).")
    write_to_neon(df, table_name)
    export_single_parquet_locally(df, table_name)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("\n✅ Spark Session Initialized for Gold Layer Transformation.\n")

    print("📥 Reading Silver Layer Tables...")
    df_patients = spark.read.parquet(f"{SILVER_BASE_PATH}/patients")
    df_doctors = spark.read.parquet(f"{SILVER_BASE_PATH}/doctors")
    df_appointments = spark.read.parquet(f"{SILVER_BASE_PATH}/appointments")
    df_treatments = spark.read.parquet(f"{SILVER_BASE_PATH}/treatments")
    df_billing = spark.read.parquet(f"{SILVER_BASE_PATH}/billing")

    print("\n🏗️ Building Dimension DataFrames...")

    # 1. Dim Patients
    dim_patients = df_patients.withColumn(
        "age_group",
        F.when(F.col("age") < 18, "Pediatric")
        .when((F.col("age") >= 18) & (F.col("age") < 60), "Adult")
        .otherwise("Senior"),
    ).select(
        "patient_id", "first_name", "last_name", "gender",
        "date_of_birth", "age", "age_group",
        "contact_number", "address", "email",
        "insurance_provider", "insurance_number", "registration_date",
    )

    # 2. Dim Doctors
    dim_doctors = df_doctors.withColumn(
        "experience_level",
        F.when(F.col("years_experience") < 5, "Junior")
        .when((F.col("years_experience") >= 5) & (F.col("years_experience") < 15), "Mid-Level")
        .otherwise("Senior"),
    ).select(
        "doctor_id", "first_name", "last_name", "specialization",
        "hospital_branch", "years_experience", "experience_level",
        "phone_number", "email",
    )

    # 3. Dim Treatments (required by Power BI model + reconciliation checks)
    dim_treatments = df_treatments.select(
        "treatment_id", "appointment_id", "treatment_type",
        "description", "cost", "treatment_date",
    )

    # 4. Dim Date
    date_bounds = (
        df_appointments.select(F.col("appointment_date").alias("d"))
        .union(df_treatments.select(F.to_date(F.col("treatment_date")).alias("d")))
        .union(df_billing.select(F.to_date(F.col("bill_date")).alias("d")))
        .agg(F.min("d").alias("min_d"), F.max("d").alias("max_d"))
        .collect()[0]
    )
    min_date, max_date = date_bounds["min_d"], date_bounds["max_d"]

    dim_date = (
        spark.range(1)
        .select(F.explode(F.sequence(F.lit(min_date), F.lit(max_date))).alias("full_date"))
        .select(
            F.col("full_date").alias("date_key"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.dayofmonth("full_date").alias("day"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        )
    )

    print("\n🏗️ Building Fact DataFrames...")

    # 4. Fact Appointments
    fact_appointments = df_appointments.select(
        F.col("appointment_id"),
        F.col("patient_id"),
        F.col("doctor_id"),
        F.col("appointment_date").alias("date_key"),
        F.col("appointment_timestamp"),
        F.col("status").alias("appointment_status"),
        F.col("is_noshow"),
        F.col("reason_for_visit"),
    )

    # 5. Fact Treatments
    fact_treatments = (
        df_treatments.alias("trt")
        .join(
            df_appointments.select(
                F.col("appointment_id").alias("_appt_id"),
                F.col("patient_id").alias("_appt_patient_id"),
                F.col("doctor_id").alias("_appt_doctor_id"),
            ),
            F.col("trt.appointment_id") == F.col("_appt_id"),
            "left",
        )
        .select(
            F.col("trt.treatment_id"),
            F.col("trt.appointment_id"),
            F.col("_appt_patient_id").alias("patient_id"),
            F.col("_appt_doctor_id").alias("doctor_id"),
            F.col("trt.treatment_type"),
            F.col("trt.description"),
            F.col("trt.cost").alias("treatment_cost"),
            F.col("trt.treatment_cost_status"),
            F.to_date(F.col("trt.treatment_date")).alias("date_key"),
        )
    )

    # 6. Fact Billing
    fact_billing = (
        df_billing.alias("bil")
        .join(
            df_treatments.select(
                F.col("treatment_id").alias("_trt_id"),
                F.col("appointment_id").alias("_trt_appt_id"),
            ),
            F.col("bil.treatment_id") == F.col("_trt_id"),
            "left",
        )
        .join(
            df_appointments.select(
                F.col("appointment_id").alias("_appt_id"),
                F.col("doctor_id").alias("_appt_doctor_id"),
            ),
            F.col("_trt_appt_id") == F.col("_appt_id"),
            "left",
        )
        .select(
            F.col("bil.bill_id"),
            F.col("bil.patient_id"),
            F.col("bil.treatment_id"),
            F.col("_appt_doctor_id").alias("doctor_id"),
            F.to_date(F.col("bil.bill_date")).alias("date_key"),
            F.col("bil.payment_method"),
            F.col("bil.payment_status"),
            F.col("bil.amount").alias("billed_amount"),
            F.col("bil.billing_status"),
            (F.col("bil.payment_status") == "Paid").alias("is_paid"),
        )
    )

    # Combined billing + treatment fact: the table the Power BI semantic
    # model and 04_reconciliation_checks.py both read. doctor_id and
    # hospital_branch are denormalized onto it so the Hospital Branch
    # comparison visuals don't need a fragile fact-to-fact relationship.
    fact_billing_treatments = (
        df_billing.alias("bil")
        .join(df_treatments.alias("trt"), F.col("bil.treatment_id") == F.col("trt.treatment_id"), "inner")
        .join(
            df_appointments.select(
                F.col("appointment_id").alias("_appt_id"),
                F.col("doctor_id").alias("_appt_doctor_id"),
            ),
            F.col("trt.appointment_id") == F.col("_appt_id"),
            "left",
        )
        .join(
            df_doctors.select(
                F.col("doctor_id").alias("_doc_id"),
                F.col("hospital_branch").alias("_doc_branch"),
            ),
            F.col("_appt_doctor_id") == F.col("_doc_id"),
            "left",
        )
        .select(
            F.col("bil.bill_id"),
            F.col("bil.patient_id"),
            F.col("bil.treatment_id"),
            F.col("trt.appointment_id"),
            F.col("_appt_doctor_id").alias("doctor_id"),
            F.col("_doc_branch").alias("hospital_branch"),
            F.col("trt.treatment_type"),
            F.to_date(F.col("bil.bill_date")).alias("date_key"),
            F.col("bil.payment_method"),
            F.col("bil.payment_status"),
            F.col("bil.amount").alias("billed_amount"),
            F.col("trt.cost").alias("treatment_cost"),
            (F.col("bil.amount") - F.col("trt.cost")).alias("revenue_variance"),
            (F.col("bil.payment_status") == "Paid").alias("is_paid"),
        )
    )

    tables_to_write = [
        (dim_patients, "dim_patients"),
        (dim_doctors, "dim_doctors"),
        (dim_treatments, "dim_treatments"),
        (dim_date, "dim_date"),
        (fact_appointments, "fact_appointments"),
        (fact_treatments, "fact_treatments"),
        (fact_billing, "fact_billing"),
        (fact_billing_treatments, "fact_billing_treatments"),
    ]

    print("\n🚀 Writing All Gold Tables in Parallel...")
    with ThreadPoolExecutor(max_workers=len(tables_to_write)) as executor:
        futures = [
            executor.submit(write_gold_table, df, table_name)
            for df, table_name in tables_to_write
        ]
        for future in futures:
            future.result()

    print("\n" + "=" * 50)
    print("🎉 GOLD LAYER PIPELINE COMPLETED SUCCESSFULLY IN PARALLEL!")
    print(f"   MinIO:        {GOLD_BASE_PATH}/")
    print(f"   Neon (Power BI source): {NEON_DB}")
    print(f"   Power BI export:        {os.path.abspath(LOCAL_EXPORT_DIR)}")
    print("=" * 50)

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        raise