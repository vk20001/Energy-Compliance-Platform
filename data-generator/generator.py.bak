import psycopg2
import random
import time
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "energydb",
    "user": "energyuser",
    "password": "energypass"
}

FACILITIES = [
    ("Stahlwerk Mannheim",       "steel",      "Baden-Württemberg",  850000.00, True),
    ("Chemiepark Ludwigshafen",  "chemicals",  "Rheinland-Pfalz",    1200000.00, True),
    ("Glaswerk Würzburg",        "glass",      "Bayern",             320000.00, True),
    ("Papierfabrik Augsburg",    "paper",      "Bayern",             290000.00, True),
    ("Raffinerie Hamburg",       "refinery",   "Hamburg",            2100000.00, True),
    ("Zementwerk Leipzig",       "cement",     "Sachsen",            410000.00, False),
    ("Aluminiumwerk Neuss",      "aluminium",  "Nordrhein-Westfalen",780000.00, True),
    ("Kupferhütte Hamburg",      "metals",     "Hamburg",            560000.00, True),
]

SECTORS_BASELINE_KWH = {
    "steel":      95000,
    "chemicals":  140000,
    "glass":      35000,
    "paper":      32000,
    "refinery":   250000,
    "cement":     48000,
    "aluminium":  90000,
    "metals":     65000,
}

INVESTMENT_CATEGORIES = ["renewable", "storage", "efficiency", "flexibility"]

CORRECTION_REASONS = [
    "Sensor calibration error",
    "Communication timeout corrected",
    "Manual verification override",
    "Meter replacement adjustment",
    "Data transmission gap filled",
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def seed_facilities(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM facilities")
    count = cur.fetchone()[0]
    if count >= len(FACILITIES):
        log.info("Facilities already seeded, skipping.")
        cur.close()
        return

    for name, sector, region, consumption, eligible in FACILITIES:
        cur.execute("""
            INSERT INTO facilities
                (name, sector, region, annual_consumption_mwh, subsidy_eligible)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (name, sector, region, consumption, eligible))
        log.info(f"Seeded facility: {name}")

    conn.commit()
    cur.close()


def get_facility_ids(conn):
    cur = conn.cursor()
    cur.execute("SELECT facility_id, sector FROM facilities")
    rows = cur.fetchall()
    cur.close()
    return rows


def get_last_timestamp(conn):
    cur = conn.cursor()
    cur.execute("SELECT MAX(reading_timestamp) FROM meter_readings")
    last_ts = cur.fetchone()[0]
    cur.close()
    return last_ts


def insert_meter_reading(conn, facility_id, sector, reading_ts):
    baseline = SECTORS_BASELINE_KWH.get(sector, 50000)
    seasonal_factor = 1.0 + 0.15 * abs(reading_ts.month - 6.5) / 6.5
    noise = random.uniform(0.92, 1.08)
    reported_kwh = round(baseline * seasonal_factor * noise, 3)

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO meter_readings
            (facility_id, reading_timestamp, reported_kwh, corrected, correction_reason)
        VALUES (%s, %s, %s, false, null)
        RETURNING reading_id
    """, (facility_id, reading_ts, reported_kwh))
    reading_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    log.info(f"Inserted reading {reading_id} for facility {facility_id}: {reported_kwh} kWh")
    return reading_id, reported_kwh


def maybe_correct_reading(conn, reading_id, original_kwh):
    if random.random() > 0.05:
        return
    correction_factor = random.uniform(0.97, 1.03)
    corrected_kwh = round(original_kwh * correction_factor, 3)
    reason = random.choice(CORRECTION_REASONS)

    cur = conn.cursor()
    cur.execute("""
        UPDATE meter_readings
        SET reported_kwh = %s,
            corrected = true,
            correction_reason = %s,
            updated_at = NOW()
        WHERE reading_id = %s
    """, (corrected_kwh, reason, reading_id))
    conn.commit()
    cur.close()
    log.info(f"Corrected reading {reading_id}: {original_kwh} -> {corrected_kwh} kWh | {reason}")


def maybe_insert_investment(conn, facility_id):
    if random.random() > 0.03:
        return
    amount = round(random.uniform(500000, 5000000), 2)
    category = random.choice(INVESTMENT_CATEGORIES)
    status = random.choice(["pending", "confirmed"])
    investment_date = datetime.now().date() - timedelta(days=random.randint(0, 30))

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO investments
            (facility_id, investment_date, amount_eur, category, status)
        VALUES (%s, %s, %s, %s, %s)
    """, (facility_id, investment_date, amount, category, status))
    conn.commit()
    cur.close()
    log.info(f"Inserted investment for facility {facility_id}: {amount} EUR | {category} | {status}")


def maybe_update_investment_status(conn):
    if random.random() > 0.02:
        return
    cur = conn.cursor()
    cur.execute("""
        SELECT investment_id FROM investments
        WHERE status = 'pending'
        ORDER BY RANDOM() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        cur.close()
        return
    investment_id = row[0]
    cur.execute("""
        UPDATE investments
        SET status = 'confirmed',
            updated_at = NOW()
        WHERE investment_id = %s
    """, (investment_id,))
    conn.commit()
    cur.close()
    log.info(f"Updated investment {investment_id} status to confirmed")


def run():
    log.info("Starting energy data generator")
    conn = get_connection()
    seed_facilities(conn)
    facilities = get_facility_ids(conn)
    last_ts = get_last_timestamp(conn)
    conn.close()

    if last_ts:
        reading_ts = last_ts.replace(tzinfo=None) + timedelta(hours=1)
        log.info(f"Resuming from {reading_ts}")
    else:
        reading_ts = datetime(2026, 1, 1, 0, 0, 0)
        log.info("Starting fresh from 2026-01-01")

    interval = timedelta(hours=1)

    while True:
        try:
            conn = get_connection()
            for facility_id, sector in facilities:
                reading_id, original_kwh = insert_meter_reading(
                    conn, facility_id, sector, reading_ts
                )
                maybe_correct_reading(conn, reading_id, original_kwh)
                maybe_insert_investment(conn, facility_id)

            maybe_update_investment_status(conn)
            conn.close()

            reading_ts += interval
            log.info(f"Completed cycle for timestamp: {reading_ts}")
            time.sleep(2)

        except Exception as e:
            log.error(f"Error in generator cycle: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run()
