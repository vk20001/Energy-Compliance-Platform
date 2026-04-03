import os
from databricks import sql
from pyspark.sql import SparkSession
import pandas as pd

# Databricks connection
DB_HOST = os.getenv("DATABRICKS_HOST")
DB_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DB_TOKEN = os.getenv("DATABRICKS_TOKEN")
CATALOG = "workspace"
SCHEMA = "energy_compliance"

# Read exported parquet files into pandas
print("Reading parquet files...")
spark = SparkSession.builder.appName("export-to-pandas") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

mr_pdf = spark.read.format("delta").load("datalake/silver/meter_readings").toPandas()
inv_pdf = spark.read.format("delta").load("datalake/silver/investments").toPandas()

from pyspark.sql.functions import col, get_json_object
fac_df = spark.read.format("delta").load("datalake/bronze/facilities")
fac_pdf = fac_df.select(
    get_json_object(col("value"), "$.payload.after.facility_id").cast("int").alias("facility_id"),
    get_json_object(col("value"), "$.payload.after.name").alias("name"),
    get_json_object(col("value"), "$.payload.after.sector").alias("sector"),
    get_json_object(col("value"), "$.payload.after.region").alias("region"),
    get_json_object(col("value"), "$.payload.after.annual_consumption_mwh").alias("annual_consumption_mwh_b64"),
    get_json_object(col("value"), "$.payload.after.subsidy_eligible").cast("boolean").alias("subsidy_eligible"),
).toPandas()

spark.stop()
print(f"Loaded: meter_readings={len(mr_pdf)}, investments={len(inv_pdf)}, facilities={len(fac_pdf)}")

# Connect to Databricks
print("Connecting to Databricks SQL Warehouse...")
conn = sql.connect(
    server_hostname=DB_HOST,
    http_path=DB_HTTP_PATH,
    access_token=DB_TOKEN,
    catalog=CATALOG,
    schema=SCHEMA,
)
cursor = conn.cursor()

# Create and load silver_meter_readings
print("Creating silver_meter_readings table...")
cursor.execute("DROP TABLE IF EXISTS silver_meter_readings")
cursor.execute("""
    CREATE TABLE silver_meter_readings (
        reading_id INT,
        facility_id INT,
        reading_timestamp TIMESTAMP,
        reported_kwh DECIMAL(12,3),
        corrected BOOLEAN,
        correction_reason STRING,
        operation STRING,
        cdc_timestamp TIMESTAMP,
        bronze_offset BIGINT,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN,
        processed_at TIMESTAMP
    )
""")

# Insert in batches
batch_size = 500
for i in range(0, len(mr_pdf), batch_size):
    batch = mr_pdf.iloc[i:i+batch_size]
    values = []
    for _, row in batch.iterrows():
        valid_to = f"'{row['valid_to']}'" if pd.notna(row['valid_to']) else "NULL"
        corr_reason = f"'{str(row['correction_reason']).replace(chr(39), chr(39)+chr(39))}'" if pd.notna(row['correction_reason']) else "NULL"
        values.append(f"""({row['reading_id']}, {row['facility_id']}, '{row['reading_timestamp']}',
            {row['reported_kwh']}, {str(row['corrected']).lower()}, {corr_reason},
            '{row['operation']}', '{row['cdc_timestamp']}', {row['bronze_offset']},
            '{row['valid_from']}', {valid_to}, {str(row['is_current']).lower()},
            '{row['processed_at']}')""")
    insert_sql = f"INSERT INTO silver_meter_readings VALUES {','.join(values)}"
    cursor.execute(insert_sql)
    print(f"  Inserted meter_readings batch {i}-{i+len(batch)}")

# Create and load silver_investments
print("Creating silver_investments table...")
cursor.execute("DROP TABLE IF EXISTS silver_investments")
cursor.execute("""
    CREATE TABLE silver_investments (
        investment_id INT,
        facility_id INT,
        investment_date DATE,
        amount_eur DECIMAL(14,2),
        category STRING,
        status STRING,
        operation STRING,
        cdc_timestamp TIMESTAMP,
        bronze_offset BIGINT,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN,
        processed_at TIMESTAMP
    )
""")

for i in range(0, len(inv_pdf), batch_size):
    batch = inv_pdf.iloc[i:i+batch_size]
    values = []
    for _, row in batch.iterrows():
        valid_to = f"'{row['valid_to']}'" if pd.notna(row['valid_to']) else "NULL"
        values.append(f"""({row['investment_id']}, {row['facility_id']}, '{row['investment_date']}',
            {row['amount_eur']}, '{row['category']}', '{row['status']}',
            '{row['operation']}', '{row['cdc_timestamp']}', {row['bronze_offset']},
            '{row['valid_from']}', {valid_to}, {str(row['is_current']).lower()},
            '{row['processed_at']}')""")
    insert_sql = f"INSERT INTO silver_investments VALUES {','.join(values)}"
    cursor.execute(insert_sql)
    print(f"  Inserted investments batch {i}-{i+len(batch)}")

# Create and load facilities
print("Creating dim_facilities table...")
cursor.execute("DROP TABLE IF EXISTS dim_facilities")
cursor.execute("""
    CREATE TABLE dim_facilities (
        facility_id INT,
        name STRING,
        sector STRING,
        region STRING,
        annual_consumption_mwh_b64 STRING,
        subsidy_eligible BOOLEAN
    )
""")

values = []
for _, row in fac_pdf.iterrows():
    name = str(row['name']).replace("'", "''")
    region = str(row['region']).replace("'", "''")
    b64 = str(row['annual_consumption_mwh_b64']) if pd.notna(row['annual_consumption_mwh_b64']) else "NULL"
    values.append(f"""({row['facility_id']}, '{name}', '{row['sector']}', '{region}',
        '{b64}', {str(row['subsidy_eligible']).lower()})""")
insert_sql = f"INSERT INTO dim_facilities VALUES {','.join(values)}"
cursor.execute(insert_sql)
print("  Inserted 8 facilities")

# Verify
print("\nVerifying uploads...")
for table in ["silver_meter_readings", "silver_investments", "dim_facilities"]:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  {table}: {count} rows")

cursor.close()
conn.close()
print("\nUpload complete.")
