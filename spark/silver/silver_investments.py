from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, current_timestamp, lit, udf, expr
)
from pyspark.sql.types import DecimalType, DateType
from delta.tables import DeltaTable
from decimal import Decimal
from datetime import date, timedelta
import base64
import os

# Paths
BRONZE_PATH = "/home/vaishu19/energy-compliance-platform/datalake/bronze/investments"
SILVER_PATH = "/home/vaishu19/energy-compliance-platform/datalake/silver/investments"

spark = (SparkSession.builder
    .appName("silver-investments-scd2")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── UDFs ─────────────────────────────────────────────────────────────
def decode_kafka_decimal(b64_str, scale=2):
    if b64_str is None:
        return None
    try:
        raw_bytes = base64.b64decode(b64_str)
        unscaled = int.from_bytes(raw_bytes, byteorder="big", signed=True)
        return Decimal(unscaled) / Decimal(10 ** scale)
    except Exception:
        return None

def epoch_days_to_date(days):
    if days is None:
        return None
    try:
        return date(1970, 1, 1) + timedelta(days=int(days))
    except Exception:
        return None

decode_amount_udf = udf(decode_kafka_decimal, DecimalType(14, 2))
epoch_days_udf = udf(epoch_days_to_date, DateType())

# ── Step 1: Read Bronze ──────────────────────────────────────────────
print("Reading Bronze investments...")
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze rows: {bronze_df.count()}")

# ── Step 2: Parse Debezium envelope ──────────────────────────────────
parsed_df = (bronze_df
    .select(
        get_json_object(col("value"), "$.payload.op").alias("operation"),
        get_json_object(col("value"), "$.payload.ts_ms").cast("long").alias("cdc_timestamp_ms"),
        get_json_object(col("value"), "$.payload.after.investment_id").cast("int").alias("investment_id"),
        get_json_object(col("value"), "$.payload.after.facility_id").cast("int").alias("facility_id"),
        get_json_object(col("value"), "$.payload.after.investment_date").alias("investment_date_raw"),
        get_json_object(col("value"), "$.payload.after.amount_eur").alias("amount_eur_b64"),
        get_json_object(col("value"), "$.payload.after.category").alias("category"),
        get_json_object(col("value"), "$.payload.after.status").alias("status"),
        col("offset").alias("bronze_offset"),
    )
    .filter(col("operation").isin("r", "c", "u"))
)

# Convert types
parsed_df = (parsed_df
    .withColumn("investment_date", epoch_days_udf(col("investment_date_raw")))
    .withColumn("amount_eur", decode_amount_udf(col("amount_eur_b64")))
    .withColumn("cdc_timestamp", (col("cdc_timestamp_ms") / 1000).cast("timestamp"))
    .drop("investment_date_raw", "amount_eur_b64", "cdc_timestamp_ms")
)

print(f"Parsed CDC events: {parsed_df.count()}")
print("Operations breakdown:")
parsed_df.groupBy("operation").count().show()

print("Sample parsed values:")
parsed_df.select("investment_id", "facility_id", "investment_date", "amount_eur", "category", "status").show(5, truncate=False)

# ── Step 3: Separate inserts and updates ─────────────────────────────
inserts_df = parsed_df.filter(col("operation").isin("r", "c"))
updates_df = parsed_df.filter(col("operation") == "u")

print(f"Inserts (r + c): {inserts_df.count()}")
print(f"Updates (u): {updates_df.count()}")

# ── Step 4: Build initial Silver table from inserts ──────────────────
silver_inserts = (inserts_df
    .select(
        "investment_id", "facility_id", "investment_date",
        "amount_eur", "category", "status",
        "operation", "cdc_timestamp", "bronze_offset"
    )
    .withColumn("valid_from", col("cdc_timestamp"))
    .withColumn("valid_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
    .withColumn("processed_at", current_timestamp())
)

silver_exists = os.path.exists(SILVER_PATH) and os.path.exists(os.path.join(SILVER_PATH, "_delta_log"))

if not silver_exists:
    print("Creating Silver table from inserts...")
    (silver_inserts.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )
    print(f"Silver table created with {silver_inserts.count()} rows.")
else:
    print("Silver table already exists, skipping initial write.")

# ── Step 5: Apply SCD Type 2 MERGE for status changes ───────────────
if updates_df.count() > 0:
    print(f"Applying SCD Type 2 MERGE for {updates_df.count()} status changes...")

    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    updates_staged = (updates_df
        .select(
            "investment_id", "facility_id", "investment_date",
            "amount_eur", "category", "status",
            "operation", "cdc_timestamp", "bronze_offset"
        )
        .withColumn("valid_from", col("cdc_timestamp"))
        .withColumn("valid_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .withColumn("processed_at", current_timestamp())
    )

    # Step 5a: Close existing current records
    silver_table.alias("silver").merge(
        updates_staged.alias("updates"),
        "silver.investment_id = updates.investment_id AND silver.is_current = true"
    ).whenMatchedUpdate(
        set={
            "valid_to": col("updates.cdc_timestamp"),
            "is_current": lit(False),
        }
    ).execute()

    # Step 5b: Insert new records with updated status
    (updates_staged.write
        .format("delta")
        .mode("append")
        .save(SILVER_PATH)
    )

    print("SCD Type 2 MERGE complete.")
else:
    print("No updates to process.")

# ── Step 6: Final validation ─────────────────────────────────────────
silver_final = DeltaTable.forPath(spark, SILVER_PATH).toDF()
total_rows = silver_final.count()
current_rows = silver_final.filter(col("is_current") == True).count()
historical_rows = silver_final.filter(col("is_current") == False).count()

print("\n=== Silver Investments Summary ===")
print(f"Total rows:      {total_rows}")
print(f"Current records: {current_rows}")
print(f"Historical rows: {historical_rows}")
print(f"Status changes:  {historical_rows}")

# Show sample status transition
print("\nSample investment status transition (history):")
changed_ids = (silver_final
    .filter(col("is_current") == False)
    .select("investment_id")
    .limit(2)
    .collect()
)
if changed_ids:
    sample_ids = [r["investment_id"] for r in changed_ids]
    (silver_final
        .filter(col("investment_id").isin(sample_ids))
        .orderBy("investment_id", "valid_from")
        .select("investment_id", "facility_id", "amount_eur", "category",
                "status", "is_current", "valid_from", "valid_to")
        .show(10, truncate=False)
    )

spark.stop()
