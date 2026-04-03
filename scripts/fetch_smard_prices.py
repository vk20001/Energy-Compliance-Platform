import os
import requests
import json
import time
from datetime import datetime, timezone
from databricks import sql
import pandas as pd

# SMARD API config
FILTER = "4169"  # Marktpreis Deutschland/Luxemburg (day-ahead wholesale price)
REGION = "DE"
RESOLUTION = "hour"

# Databricks config
DB_HOST = os.getenv("DATABRICKS_HOST")
DB_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DB_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Step 1: Get available timestamps
print("Fetching available timestamps from SMARD...")
index_url = f"https://www.smard.de/app/chart_data/{FILTER}/{REGION}/index_{RESOLUTION}.json"
resp = requests.get(index_url, timeout=30)
resp.raise_for_status()
timestamps = resp.json()["timestamps"]
print(f"Found {len(timestamps)} weekly timestamp blocks")

# Step 2: Filter for Jan-Mar 2026
# Each timestamp covers one week of hourly data
start_2026 = int(datetime(2025, 12, 29, tzinfo=timezone.utc).timestamp() * 1000)  # week containing Jan 1
end_2026_q1 = int(datetime(2026, 3, 23, tzinfo=timezone.utc).timestamp() * 1000)

relevant_ts = [t for t in timestamps if start_2026 <= t <= end_2026_q1]
print(f"Relevant timestamps for Jan-Mar 2026: {len(relevant_ts)}")

# Step 3: Fetch hourly prices for each week
all_prices = []
for ts in relevant_ts:
    url = f"https://www.smard.de/app/chart_data/{FILTER}/{REGION}/{FILTER}_{REGION}_{RESOLUTION}_{ts}.json"
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        print(f"  Skipped timestamp {ts}: HTTP {resp.status_code}")
        continue
    data = resp.json()
    series = data.get("series", [])
    for entry in series:
        ts_ms, price = entry[0], entry[1]
        if price is not None:
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            # Only keep Jan 1 - Mar 21 2026
            if datetime(2026, 1, 1, tzinfo=timezone.utc) <= dt <= datetime(2026, 3, 22, tzinfo=timezone.utc):
                all_prices.append({
                    "price_timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "price_eur_per_mwh": round(price, 2),
                    "price_eur_per_kwh": round(price / 1000, 6),
                })
    print(f"  Fetched {ts} -> {len(series)} entries")
    time.sleep(0.5)  # be polite to the API

print(f"\nTotal hourly prices collected: {len(all_prices)}")

if len(all_prices) == 0:
    print("No prices found. Check SMARD API availability.")
    exit(1)

# Show sample
df = pd.DataFrame(all_prices)
print(f"\nDate range: {df['price_timestamp'].min()} to {df['price_timestamp'].max()}")
print(f"Avg price: {df['price_eur_per_mwh'].mean():.2f} EUR/MWh ({df['price_eur_per_kwh'].mean():.4f} EUR/kWh)")
print(f"Min price: {df['price_eur_per_mwh'].min():.2f} EUR/MWh")
print(f"Max price: {df['price_eur_per_mwh'].max():.2f} EUR/MWh")

# Step 4: Upload to Databricks
print("\nUploading to Databricks...")
conn = sql.connect(
    server_hostname=DB_HOST,
    http_path=DB_HTTP_PATH,
    access_token=DB_TOKEN,
    catalog="workspace",
    schema="energy_compliance",
)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS smard_wholesale_prices")
cursor.execute("""
    CREATE TABLE smard_wholesale_prices (
        price_timestamp TIMESTAMP,
        price_eur_per_mwh DECIMAL(10,2),
        price_eur_per_kwh DECIMAL(10,6)
    )
""")

# Insert in batches
batch_size = 500
for i in range(0, len(all_prices), batch_size):
    batch = all_prices[i:i+batch_size]
    values = [f"('{r['price_timestamp']}', {r['price_eur_per_mwh']}, {r['price_eur_per_kwh']})" for r in batch]
    cursor.execute(f"INSERT INTO smard_wholesale_prices VALUES {','.join(values)}")
    print(f"  Inserted batch {i}-{i+len(batch)}")

cursor.execute("SELECT COUNT(*) FROM smard_wholesale_prices")
count = cursor.fetchone()[0]
print(f"\nsmard_wholesale_prices: {count} rows uploaded")

cursor.close()
conn.close()
print("Done.")
