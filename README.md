# Healthcare Analytics & Lakehouse Platform

An end-to-end, production-grade **Healthcare Operations & Financial Analytics Platform**, built on a Medallion (Bronze → Silver → Gold) Lakehouse architecture. The pipeline extracts operational data from a live PostgreSQL source, enforces automated data quality gates, models a dimensional star schema, and delivers a 3-page executive Power BI report — all orchestrated end-to-end by Apache Airflow.

![Airflow DAG — all tasks succeeding](assets/airflow_dag_success.png)

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Quality & Governance](#data-quality--governance)
- [Gold Layer — Star Schema](#gold-layer--star-schema)
- [Orchestration](#orchestration)
- [Power BI Dashboard](#power-bi-dashboard)
- [Getting Started](#getting-started)
- [Reconciliation & Data Integrity](#reconciliation--data-integrity)
- [Known Assumptions & Limitations](#known-assumptions--limitations)

---

## Architecture

```
Neon PostgreSQL (source)
        │  JDBC extract
        ▼
   BRONZE  (MinIO, s3a://hmd-lakehouse/bronze/)
        │  raw, immutable, append-only, partitioned by ingestion date
        ▼
 DATA QUALITY GATEWAY  ──▶  QUARANTINE  (failed records + reason logs)
        │  cleaned, deduplicated, referentially validated
        ▼
   SILVER  (MinIO, s3a://hmd-lakehouse/silver/)
        │  star-schema transformation
        ▼
   GOLD    (MinIO, s3a://hmd-lakehouse/gold/  +  local Parquet export)
        │
        ▼
   Power BI  (3-page interactive report)
```

Every stage is orchestrated by a single Airflow DAG (`hmd_medallion_etl_pipeline`) with real health checks, automated reconciliation, and a Power BI refresh trigger as the final step.

## Tech Stack

| Layer | Technology |
|---|---|
| Source Database | Neon PostgreSQL |
| Object Storage / Lakehouse | MinIO (S3-compatible) |
| Processing Engine | PySpark (S3A connector) |
| Storage Format | Parquet |
| Orchestration | Apache Airflow (LocalExecutor + dedicated Postgres metadata DB) |
| Data Quality & Governance | Custom PySpark DQ Gateway |
| BI & Reporting | Power BI (Get Data → Python / Parquet) |
| Containerization | Docker Compose |

## Project Structure

```
healthcare-analytics-lakehouse/
├── dags/
│   └── hmd_medallion_etl_pipeline.py     # Airflow DAG definition
├── scripts/
│   ├── 00_test_connections.py            # Ad-hoc connectivity smoke test
│   ├── 01_bronze_ingestion.py            # Neon -> MinIO Bronze (append-only)
│   ├── 02_silver_ingestion.py            # DQ Gateway: Bronze -> Silver + Quarantine
│   ├── 03_gold_transformation.py         # Silver -> Gold star schema
│   └── 04_reconciliation_checks.py       # End-to-end row-count & financial reconciliation
├── output/                               # Local Parquet export consumed by Power BI
├── reports/                              # Power BI Project (.pbip) — semantic model + report
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── .env                                  # Not committed — see Getting Started
```

## Data Quality & Governance

Every table passes through a central **DQ Gateway** (`apply_dq_gateway`) before it's allowed into Silver. The gateway enforces:

| Rule | Description |
|---|---|
| **Uniqueness** | Primary keys must be non-null and unique. Duplicates from re-ingestion are resolved by keeping the latest `_ingested_at` per key. |
| **Date Sanity** | `appointment_date` cannot be set in the future. |
| **Positive Financials** | `billed_amount` and `treatment_cost` must be strictly greater than zero. |
| **Domain Enums** | `payment_status` must be one of `Paid`, `Pending`, `Failed`, `Refunded`. |
| **Referential Integrity** | Every foreign key (`patient_id`, `doctor_id`, `treatment_id`, ...) must resolve to a real parent row. |

Every rejected row is routed to `quarantine/failed_records/<table>/` with a full `_dq_failure_reason`, `_quarantined_at`, and `_source_table` audit trail — nothing is silently dropped.

## Gold Layer — Star Schema

| Dimension | Fact |
|---|---|
| `dim_patients` (age, age_group, insurance...) | `fact_appointments` (status, is_noshow, reason_for_visit) |
| `dim_doctors` (specialization, hospital_branch, experience_level) | `fact_billing_treatments` (billed_amount, treatment_cost, **revenue_variance**) |
| `dim_treatments` | |
| `dim_date` | |

`revenue_variance = billed_amount - treatment_cost` is calculated directly in the Gold layer and independently re-verified in the reconciliation script.

## Orchestration

The Airflow DAG runs six sequential tasks, each gating the next on success:

1. **`pre_run_check`** — live connectivity check against Neon, MinIO health endpoint, and target bucket
2. **`pyspark_extraction`** — Bronze ingestion
3. **`data_quality_rules`** — Silver DQ Gateway
4. **`gold_transformations`** — star schema build
5. **`reconciliation_checks`** — end-to-end row-count & financial reconciliation (fails the run on any mismatch)
6. **`power_bi_refresh`** — triggers a Power BI Service dataset refresh via the REST API

A failure at any stage blocks every task downstream — a broken pipeline never silently reports success.

## Power BI Dashboard

Three interactive pages, built on the Gold star schema:

### Page 1 — Executive Command Center Overview
![Executive Command Center](assets/dashboard_page1_executive.png)

### Page 2 — Financial Integrity & Billing Operations
![Financial Integrity](assets/dashboard_page2_financial.png)

### Page 3 — Operational Efficiency & Capacity Planning
![Operational Efficiency](assets/dashboard_page3_operational.png)

## Getting Started

**1. Configure environment**

Copy the example and fill in your own credentials — never commit real secrets:

```bash
cp .env.example .env
```

Required variables: `NEON_HOST/PORT/DB/USER/PASSWORD`, `MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY`, `AIRFLOW_FERNET_KEY` (generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`), and optionally `POWERBI_TENANT_ID/CLIENT_ID/CLIENT_SECRET/DATASET_ID` for the automated refresh step.

**2. Launch the stack**

```bash
docker compose up airflow-init   # one-time: migrates the metadata DB, creates the admin user
docker compose up -d
```

**3. Trigger the pipeline**

Open the Airflow UI at `http://localhost:8082` (default login `admin` / `admin`) and trigger `hmd_medallion_etl_pipeline`.

**4. Connect Power BI**

Point Power BI Desktop's Python script connector (or the Parquet connector) at the `./output/` folder produced by the Gold layer. See `reports/` for the ready-to-open Power BI Project.

## Reconciliation & Data Integrity

`04_reconciliation_checks.py` runs as the final gate before any BI refresh, and independently re-verifies (not just trusts) the entire pipeline:

- **Bronze → Silver**: raw Bronze row count/financial sum must exactly equal `Silver + Quarantine`
- **Silver → Gold**: row counts and financial sums must match exactly (zero tolerance — this join is lossless by construction)
- **Formula sanity**: `SUM(revenue_variance)` is independently recomputed as `SUM(billed_amount) - SUM(treatment_cost)` and compared against the stored value

Any mismatch fails the Airflow task immediately, rather than allowing a silently incomplete dataset to reach the dashboard.

## Known Assumptions & Limitations

- **Gross Margin shows $0.00**: the source dataset's `billing.amount` and `treatments.cost` are identical for every transaction, so billed revenue exactly equals treatment cost. This is a property of the sample data, not a modeling error.
- **"Capacity Utilization"** is reported as actual doctor workload (appointments, completions, cancellations, no-shows) rather than a percentage against a fixed capacity, since the source data has no explicit daily-capacity target per doctor.
- **Power BI connectivity**: Power BI Desktop has no native connector for MinIO or generic S3-compatible storage. The Gold layer is therefore written twice — once to MinIO (satisfying the lakehouse architecture requirement) and once as local Parquet files consumed directly by Power BI.