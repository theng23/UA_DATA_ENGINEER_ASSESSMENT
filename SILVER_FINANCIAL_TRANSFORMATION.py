from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql import Row
from datetime import datetime, timezone
import re

# Set legacy time parser to handle abbreviated month names like "Feb 2024"
# Spark 3.0+ strict parser cannot parse MMM yyyy format without this setting
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

ingestion_ts = datetime.now(timezone.utc).isoformat()

# =========================================================
# STEP 1: INGEST BOTH FILES
# =========================================================
financial_long_df = spark.read.option("header", True).csv(
    "Files/data/raw/financial_long_format.csv"
)
financial_wide_df = spark.read.option("header", True).csv(
    "Files/data/raw/financial_wide_format.csv"
)
exchange_rates_df = spark.read.option("header", True).csv(
    "Files/data/raw/exchange_rates_reference.csv"
)

print(f"Long rows: {financial_long_df.count()}")
print(f"Wide rows: {financial_wide_df.count()}")
print(f"Exchange rates rows: {exchange_rates_df.count()}")

display(financial_long_df)
display(financial_wide_df)
display(exchange_rates_df)


# =========================================================
# STEP 2: UNPIVOT WIDE FORMAT
# Wide columns like 31/01/2024, 28/02/2024 -> unpivot to (period, amount)
# Parse date format dd/MM/yyyy -> YYYY-MM
# Drop rows where amount is null after unpivot (sparse wide format)
# =========================================================
date_columns = [
    c for c in financial_wide_df.columns
    if re.match(r"\d{2}/\d{2}/\d{4}", c)
]
non_date_cols = [c for c in financial_wide_df.columns if c not in date_columns]

stack_expr = ", ".join([f"'{c}', `{c}`" for c in date_columns])

financial_wide_unpivoted = (
    financial_wide_df
    .selectExpr(
        *non_date_cols,
        f"stack({len(date_columns)}, {stack_expr}) as (period_raw, amount)"
    )
    .withColumn(
        "period",
        F.date_format(F.to_date(F.col("period_raw"), "dd/MM/yyyy"), "yyyy-MM")
    )
    .drop("period_raw")
    .filter(F.col("amount").isNotNull())
    .withColumn("SOURCE_FILE", F.lit("financial_wide_format.csv"))
)

financial_long_df = financial_long_df.withColumn(
    "SOURCE_FILE", F.lit("financial_long_format.csv")
)

print("=== WIDE AFTER UNPIVOT ===")
display(financial_wide_unpivoted)


# =========================================================
# STEP 3: UNION LONG + WIDE
# Both sources now share the same schema
# Cleaning applied once on combined DataFrame
# =========================================================
combined_df = financial_long_df.unionByName(
    financial_wide_unpivoted,
    allowMissingColumns=True
)

print(f"Combined row count before cleaning: {combined_df.count()}")


# =========================================================
# STEP 4: OCR FIX — replace letter O with digit 0 in amount
# Only apply when value looks like a number
# Log every fix: original value, corrected value, order identifier
#
# Additional issue found (not in known list):
# amount column may have leading/trailing spaces — handled in Step 5
# =========================================================
combined_df = combined_df.withColumn(
    "__amount_before", F.col("amount")
).withColumn(
    "amount",
    F.when(
        F.col("amount").isNotNull() &
        F.col("amount").rlike(r"^[0-9Oo\.\,\-\s]+$"),
        F.regexp_replace(F.col("amount"), r"[Oo]", "0")
    ).otherwise(F.col("amount"))
)

df_ocr_log = (
    combined_df
    .filter(
        F.coalesce(F.col("__amount_before").cast("string"), F.lit("")) !=
        F.coalesce(F.col("amount").cast("string"), F.lit(""))
    )
    .select(
        "order_no",
        F.lit("amount").alias("COLUMN_NAME"),
        F.col("__amount_before").cast("string").alias("ORIGINAL_VALUE"),
        F.col("amount").cast("string").alias("CORRECTED_VALUE"),
        F.lit("OCR_FIX").alias("LOG_TYPE"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

combined_df = combined_df.drop("__amount_before")

print("=== OCR FIX LOG ===")
display(df_ocr_log)


# =========================================================
# STEP 5: NORMALIZE WHITESPACE + CASING
# - customer_name: trim + collapse spaces + lowercase
#   (lowercase first so canonical map in Step 6 can match reliably)
# - category: trim + collapse spaces + initcap
# - amount: trim leading/trailing spaces (additional issue found)
# =========================================================
combined_df = combined_df.withColumn(
    "customer_name",
    F.lower(F.trim(F.regexp_replace(F.col("customer_name"), r"\s+", " ")))
).withColumn(
    "category",
    F.initcap(F.trim(F.regexp_replace(F.col("category"), r"\s+", " ")))
).withColumn(
    "amount",
    F.trim(F.col("amount"))
)


# =========================================================
# STEP 6: STANDARDISE PERIOD COLUMN
# Handle all 5 known formats, output YYYY-MM
#
#   2024-01      -> yyyy-MM
#   2024/01      -> yyyy/MM
#   January 2024 -> MMMM yyyy
#   01-2024      -> MM-yyyy
#   Feb 2024     -> MMM yyyy  (requires LEGACY timeParserPolicy)
#
# Chain when().when() — first format that parses wins
# Fallback: keep original and flag as PERIOD_PARSE_FAIL
# =========================================================
date_formats = [
    "yyyy-MM",
    "yyyy/MM",
    "MMMM yyyy",
    "MM-yyyy",
    "MMM yyyy"
]

combined_df = combined_df.withColumn("__period_raw", F.col("period"))

period_expr = None
for fmt in date_formats:
    parsed = F.to_date(F.col("__period_raw"), fmt)
    formatted = F.date_format(parsed, "yyyy-MM")
    if period_expr is None:
        period_expr = F.when(parsed.isNotNull(), formatted)
    else:
        period_expr = period_expr.when(parsed.isNotNull(), formatted)

period_expr = period_expr.otherwise(F.col("__period_raw"))
combined_df = combined_df.withColumn("period", period_expr)

# Flag rows where period could not be parsed
df_period_fail_log = (
    combined_df
    .filter(F.col("period") == F.col("__period_raw"))
    .filter(~F.col("period").rlike(r"^\d{4}-\d{2}$"))
    .select(
        "order_no",
        F.lit("period").alias("COLUMN_NAME"),
        F.col("__period_raw").alias("ORIGINAL_VALUE"),
        F.lit(None).cast("string").alias("CORRECTED_VALUE"),
        F.lit("PERIOD_PARSE_FAIL").alias("LOG_TYPE"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

combined_df = combined_df.drop("__period_raw")

print("=== PERIOD PARSE FAIL LOG ===")
display(df_period_fail_log)


# =========================================================
# STEP 7: NORMALISE CUSTOMER NAME
#
# Normalisation logic:
#   1. Lowercase in Step 5 ensures consistent matching
#   2. Explicit map resolves known variants to canonical form
#   3. Unknown names fall back to title() — at least consistently cased
#
# Why explicit map instead of just initcap:
#   "sunrise apparel" / "Sunrise Apparel" / "SUNRISE APPAREL" are the same
#   entity. initcap alone cannot resolve cross-record entity matching.
#   The map must be maintained as new variants are discovered.
#
# Known variants:
#   "sunrise apparel"      -> "Sunrise Apparel"
#   "sunrise apparel inc." -> "Sunrise Apparel"
#   "sunrise garments"     -> "Sunrise Apparel"  (same entity, different name)
#   "SUNRISE APPAREL"      -> lowercased to "sunrise apparel" in Step 5, then mapped
# =========================================================
customer_name_map = {
    "sunrise apparel": "Sunrise Apparel",
    "sunrise apparel inc.": "Sunrise Apparel",
    "sunrise garments": "Sunrise Apparel",
}

def normalize_customer_name(name):
    if name is None:
        return None
    return customer_name_map.get(name.strip().lower(), name.strip().title())

normalize_customer_name_udf = F.udf(normalize_customer_name, StringType())

combined_df = combined_df.withColumn(
    "__customer_before", F.col("customer_name")
).withColumn(
    "customer_name",
    normalize_customer_name_udf(F.col("customer_name"))
)

df_customer_norm_log = (
    combined_df
    .filter(
        F.coalesce(F.col("__customer_before"), F.lit("")) !=
        F.coalesce(F.col("customer_name"), F.lit(""))
    )
    .select(
        "order_no",
        F.lit("customer_name").alias("COLUMN_NAME"),
        F.col("__customer_before").alias("ORIGINAL_VALUE"),
        F.col("customer_name").alias("CORRECTED_VALUE"),
        F.lit("CUSTOMER_NORM").alias("LOG_TYPE"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

combined_df = combined_df.drop("__customer_before")

print("=== CUSTOMER NORMALISATION LOG ===")
display(df_customer_norm_log)


# =========================================================
# STEP 8: CAST AMOUNT TO NUMERIC
# After OCR fix, amount should be parseable as double
# Flag rows where cast still fails — additional data quality issue
# =========================================================
combined_df = combined_df.withColumn(
    "amount_numeric",
    F.col("amount").cast("double")
).withColumn(
    "IS_AMOUNT_UNPARSEABLE",
    F.when(
        F.col("amount").isNotNull() & F.col("amount_numeric").isNull(),
        F.lit(True)
    ).otherwise(F.lit(False))
)

df_unparseable_log = (
    combined_df
    .filter(F.col("IS_AMOUNT_UNPARSEABLE") == True)
    .select(
        "order_no", "category", "period",
        F.col("amount").alias("RAW_AMOUNT"),
        F.lit("AMOUNT_NOT_NUMERIC").alias("LOG_TYPE"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

print("=== UNPARSEABLE AMOUNT LOG ===")
display(df_unparseable_log)


# =========================================================
# STEP 9: FLAG ORD-099
#
# ORD-099 exists in financial file but has no matching order in ERP API.
# Do NOT drop — flag with IS_ORPHANED_ORDER = True.
#
# What downstream consumers must do with ORD-099:
#   1. Exclude from fact_order_cost joins — no matching ERP order exists
#   2. Route to a quarantine / exception table for manual review
#   3. Investigate: was ORD-099 deleted from ERP, never created, or a data
#      entry error? Resolution must come from the business, not the pipeline.
#   4. Do not aggregate ORD-099 amounts into any cost summary report until
#      the orphan is resolved.
# =========================================================
combined_df = combined_df.withColumn(
    "IS_ORPHANED_ORDER",
    F.when(F.col("order_no") == "ORD-099", F.lit(True)).otherwise(F.lit(False))
)

print("=== ORD-099 FLAGGED ROWS ===")
display(combined_df.filter(F.col("IS_ORPHANED_ORDER") == True))


# =========================================================
# STEP 10: DEDUPLICATE
#
# ORD-005 OUTSOURCE appears twice with different casing and amount typo.
# After OCR fix and normalisation, both rows may still differ in amount.
#
# Strategy: keep row with HIGHER amount — conservative for cost reporting.
# Log all dropped rows.
#
# Dedup key: order_no + category + period
# (customer_name intentionally excluded — already normalised)
# =========================================================
dedup_key = ["order_no", "category", "period"]

w_dedup = Window.partitionBy(*dedup_key).orderBy(F.col("amount_numeric").desc())

combined_df = (
    combined_df
    .withColumn("__dedup_rank", F.row_number().over(w_dedup))
    .withColumn(
        "IS_DUPLICATE",
        F.when(F.col("__dedup_rank") > 1, F.lit(True)).otherwise(F.lit(False))
    )
)

df_dedup_log = (
    combined_df
    .filter(F.col("IS_DUPLICATE") == True)
    .select(
        "order_no", "category", "period", "amount", "customer_name",
        F.lit("DUPLICATE_DROP").alias("LOG_TYPE"),
        F.lit("Kept row with higher amount_numeric; dedup key: order_no+category+period").alias("DETAILS"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

combined_df = (
    combined_df
    .filter(F.col("IS_DUPLICATE") == False)
    .drop("__dedup_rank")
)

print("=== DEDUP LOG ===")
display(df_dedup_log)
print(f"Row count after dedup: {combined_df.count()}")


# =========================================================
# STEP 11: FX CONVERSION — convert all amounts to VND
#
# Strategy:
#   1. Ensure VND row exists in reference with rate_to_vnd = 1.0
#      (VND × 1 = VND — no change, but avoids null after left join)
#   2. Join on currency = currency_code
#   3. converted_amount_vnd = amount_numeric * rate_to_vnd
#   4. Flag rows where conversion fails
#
# Risk of hardcoded rates in production:
#   - Exchange rates change daily — hardcoded rates produce stale conversions
#   - A rate frozen at 2024-01 applied to 2024-12 data is silently wrong
#   - No audit trail of which rate was used for which period
#   - Production solution: fetch live rates from a provider (e.g. Open Exchange
#     Rates API) at pipeline run time, store in a rate history table,
#     join on currency + effective_date closest to the transaction period
# =========================================================
vnd_exists = exchange_rates_df.filter(F.col("currency_code") == "VND").count() > 0

if not vnd_exists:
    vnd_row = spark.createDataFrame([
        Row(
            currency="Vietnamese Dong",
            currency_code="VND",
            rate_to_vnd="1.0",
            effective_date=None,
            rate_type="FIXED"
        )
    ])
    exchange_rates_df = exchange_rates_df.unionByName(
        vnd_row, allowMissingColumns=True
    )
    print("Added VND row with rate_to_vnd = 1.0")

fx_ref = exchange_rates_df.select(
    F.col("currency_code"),
    F.col("rate_to_vnd").cast("double").alias("rate_to_vnd")
)

combined_df = (
    combined_df
    .join(fx_ref, combined_df["currency"] == fx_ref["currency_code"], how="left")
    .withColumn(
        "converted_amount_vnd",
        F.when(
            F.col("amount_numeric").isNotNull() & F.col("rate_to_vnd").isNotNull(),
            F.round(F.col("amount_numeric") * F.col("rate_to_vnd"), 2)
        ).otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "IS_FX_CONVERSION_FAILED",
        F.when(F.col("converted_amount_vnd").isNull(), F.lit(True)).otherwise(F.lit(False))
    )
    .drop("currency_code", "rate_to_vnd")
)

df_fx_fail_log = (
    combined_df
    .filter(F.col("IS_FX_CONVERSION_FAILED") == True)
    .select(
        "order_no", "category", "period", "currency", "amount",
        F.lit("FX_CONVERSION_FAILED").alias("LOG_TYPE"),
        F.lit("No matching rate in exchange_rates_reference or amount is null").alias("DETAILS"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

print("=== FX CONVERSION FAIL LOG ===")
display(df_fx_fail_log)


# =========================================================
# STEP 12: ADD INGESTION METADATA + FINAL FLAGS
# =========================================================
combined_df = combined_df.withColumn("INGESTION_TS", F.lit(ingestion_ts))

# =========================================================
# PREVIEW FINAL SILVER OUTPUT
# =========================================================
print("=== FINANCIAL SILVER — FINAL ===")
display(combined_df)

print("\n=== SUMMARY ===")
print(f"Total rows: {combined_df.count()}")
print(f"Orphaned orders (ORD-099): {combined_df.filter(F.col('IS_ORPHANED_ORDER') == True).count()}")
print(f"FX conversion failed: {combined_df.filter(F.col('IS_FX_CONVERSION_FAILED') == True).count()}")
print(f"Unparseable amounts: {combined_df.filter(F.col('IS_AMOUNT_UNPARSEABLE') == True).count()}")
