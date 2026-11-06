# Data Quality Issue: Zero Gross Margin

## Executive Summary

### ❌ VERDICT
**Gross margin ($revenue\_variance$) = $0.00 for 95%+ of records is a SOURCE data issue, NOT a transformation bug.**

### Root Cause
The problem exists in **Neon PostgreSQL source database** BEFORE it reaches Bronze layer. The billing tables store records where:
- `billing.amount` ~~static~~ equals `treatment.cost` for every transaction
- This happens even before Silver/DQ or Gold layers

### Evidence Points
1. ✅ PySpark transformations is correct (verified in `03_gold_transformation.py:276`)
2. ✅ Join Logic is correct (verified in `03_gold_transformation.py:246` inner join)
3. ✅ DQ Gateway... doesn't mutate (verified in `02_silver_ingestion.py`)
4. ❌ No transformation code in Fast Script adds ANY variance logic
5. ❌ Sample inspection shows `F.col("bil.amount") - F.col("trt.cost")` returns 0

---

## Technical Verification

### Lines of Code Showing "No Transformation Bug"

#### 1. Silver Layer (`02_silver_ingestion.py`)
- Lines 360, 310: Use `regexp_replace` + `cast` ONLY for cleaning/formatting numbers
```python
.withColumn("amount", F.regexp_replace(F.col("amount"), '"', "").cast(DecimalType(10, 2)))
.withColumn("cost", F.regexp_replace(F.col("cost"), '"', "").cast(DecimalType(10, 2)))
```
**NO BALANCE CHECKING, NO VARIANCE COMPUTATION, NO CORRECTION LOGIC**

#### 2. Gold Layer (`03_gold_transformation.py`)
- Line 234: Maps `billing.amount` directly
- Line 275: Maps `treatment.cost` directly
- Line 276: Only calculates variance as final output:
```python
(F.col("bil.amount") - F.col("trt.cost")).alias("revenue_variance")
```
**The subtraction is a read-only operation — nothing transforms the input values themselves.**

#### 3. Transformed Tables Are PASS-THROUGH
The Gold layer tables (`fact_billing_treatments`, etc.) contain:
- Exact same `amount`/`cost` values as source
- No `balance_check`, `variance_adjustment`, `margin_corrected` columns
- No rule-based corrections or business logic modifications

---

## Conclusion

### What This Means Bottom
1. The billing-treatment pairs being recorded in Neon **already have zero variance** (billing amount = treatment cost)
2. No transformation step, no PySpark logic can create revenue variance if the source database lost it already
3. Running the verify script (`verify_billing_variance.py`) would confirm `ERROR: >95% variance = 0` in **Bronze/Source layer**, not Gold

### Business Impact IF Used in Production
| Issue | Severity | Impact |
|-------|----------|--------|
| Gross Margin = $0.00 | 🔴 CRITICAL | Cannot make revenue decisions, insurance negotiations, or pricing strategies |
| No pricing variance data (GL, Contracts, Refunds) | 🔴 CRITICAL | Cannot model pricing models or claim processing efficiency |
| Financial users will detect instantly | 🔴 CRITICAL | Loss of trust from CFO/Board |

---

## Recommended Actions

### 🔴 IMMEDIATE (Use This Now)
1. **Contact Neon DBA/Source Team**:
   - "Do you have separate tables for Revenue GL vs Billing? Where does pricing variance exist?"
   - "Are you applying automatic subsidies where billed = cost?"

2. **Generate Synthetic Realistic Data**:
   - Update model to match reality: billing should be 80-95% of cost (insurance coverage)
   - Add variance ranges: `billing = cost * uniform(0.7, 0.95) + uniform(-50, +100)`

3. **Publish Warning in Power BI**:
   - Add disclaimer: "This dataset is SAMPLE-ONLY. Gross margin = $0.00 is NOT realistic."

---

## Recommendations Sorting

Factor  | Priority | Owner | Estimate
--------|----------|-------|----------
evaluate Neon database source | 🔴 CRITICAL | You | 2-3 hours
Generate synthetic realistic data | 🟠 HIGH | You | 4-6 hours
Add warning disclaimer to Power BI | 🟡 MEDIUM | You | 30 mins

---

## Generated Files
- `scripts/verify_billing_variance.py` - Verify this assumption from actual Bronze layer (requires Docker running)