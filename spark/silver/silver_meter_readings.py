from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, current_timestamp, lit, udf, to_timestamp
)
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable
from decimal import Decimal
import base64
import os

# Paths
BRONZE_PATH = "/home/vaishu19/energy-compliance-platform/datalake/bronze/meter_readings"
SILVER_PATH = "/home/vaishu19/energy-compliance-platform/datalake/silver/meter_readings"

spark = (SparkSession.builder
    .appName("silver-meter-readings-scd2")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── UDF: Decode Kafka Connect Decimal (base64 BigInteger with scale) ─
def decode_kafka_decimal(b64_str, scale=3):
    if b64_str is None:
        return None
    try:
        raw_bytes = base64.b64decode(b64_str)
        unscaled = int.from_bytes(raw_bytes, byteorder="big", signed=True)
        return Decimal(unscaled) / Decimal(10 ** scale)
    except Exception:
        return None

decode_decimal_udf = udf(decode_kafka_decimal, DecimalType(12, 3))

# ── Step 1: Read Bronze ──────────────────────────────────────────────
print("Reading Bronze layer...")
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze rows: {bronze_df.count()}")

# ── Step 2: Parse Debezium envelope using get_json_object ────────────
# This avoids schema mismatch issues with from_json on complex types
parsed_df = (bronze_df
    .select(
        get_json_object(col("value"), "$.payload.op").alias("operation"),
        get_json_object(col("value"), "$.payload.ts_ms").cast("long").alias("cdc_timestamp_ms"),
        get_json_object(col("value"), "$.payload.after.reading_id").cast("int").alias("reading_id"),
        get_json_object(col("value"), "$.payload.after.facility_id").cast("int").alias("facility_id"),
        get_json_object(col("value"), "$.payload.after.reading_timestamp").alias("reading_timestamp_str"),
        get_json_object(col("value"), "$.payload.after.reported_kwh").alias("reported_kwh_b64"),
        get_json_object(col("value"), "$.payload.after.corrected").cast("boolean").alias("corrected"),
        get_json_object(col("value"), "$.payload.after.correction_reason").alias("correction_reason"),
        col("offset").alias("bronze_offset"),
    )
    .filter(col("operation").isin("r", "c", "u"))
)

# Convert types
parsed_df = (parsed_df
    .withColumn("reading_timestamp",
                to_timestamp(col("reading_timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))
    .withColumn("reported_kwh",
                decode_decimal_udf(col("reported_kwh_b64")))
    .withColumn("cdc_timestamp",
                (col("cdc_timestamp_ms") / 1000).cast("timestamp"))
    .drop("reading_timestamp_str", "reported_kwh_b64", "cdc_timestamp_ms")
)

print(f"Parsed CDC events: {parsed_df.count()}")
print("Operations breakdown:")
parsed_df.groupBy("operation").count().show()

# Verify reported_kwh is no longer NULL
print("Sample parsed values:")
parsed_df.select("reading_id", "reading_timestamp", "reported_kwh", "corrected").show(5, truncate=False)

# ── Step 3: Separate inserts and updates ─────────────────────────────
inserts_df = parsed_df.filter(col("operation").isin("r", "c"))
updates_df = parsed_df.filter(col("operation") == "u")

print(f"Inserts (r + c): {inserts_df.count()}")
print(f"Updates (u): {updates_df.count()}")

# ── Step 4: Build initial Silver table from inserts ──────────────────
silver_inserts = (inserts_df
    .select(
        "reading_id", "facility_id", "reading_timestamp",
        "reported_kwh", "corrected", "correction_reason",
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

# ── Step 5: Apply SCD Type 2 MERGE for updates (corrections) ────────
if updates_df.count() > 0:
    print(f"Applying SCD Type 2 MERGE for {updates_df.count()} corrections...")

    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    updates_staged = (updates_df
        .select(
            "reading_id", "facility_id", "reading_timestamp",
            "reported_kwh", "corrected", "correction_reason",
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
        "silver.reading_id = updates.reading_id AND silver.is_current = true"
    ).whenMatchedUpdate(
        set={
            "valid_to": col("updates.cdc_timestamp"),
            "is_current": lit(False),
        }
    ).execute()

    # Step 5b: Insert new corrected records
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

print("\n=== Silver Layer Summary ===")
print(f"Total rows:      {total_rows}")
print(f"Current records: {current_rows}")
print(f"Historical rows: {historical_rows}")
print(f"Correction rate: {historical_rows / current_rows * 100:.1f}%")

# Show a sample corrected reading with both old and new records
print("\nSample corrected reading (showing history):")
corrected_ids = (silver_final
    .filter(col("is_current") == False)
    .select("reading_id")
    .limit(2)
    .collect()
)
if corrected_ids:
    sample_ids = [r["reading_id"] for r in corrected_ids]
    (silver_final
        .filter(col("reading_id").isin(sample_ids))
        .orderBy("reading_id", "valid_from")
        .select("reading_id", "facility_id", "reported_kwh", "corrected",
                "correction_reason", "is_current", "valid_from", "valid_to")
        .show(10, truncate=False)
    )

spark.stop()
