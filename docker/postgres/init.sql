CREATE TABLE IF NOT EXISTS facilities (
    facility_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    region VARCHAR(100),
    annual_consumption_mwh NUMERIC(12,3),
    subsidy_eligible BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS meter_readings (
    reading_id SERIAL PRIMARY KEY,
    facility_id INTEGER REFERENCES facilities(facility_id),
    reading_timestamp TIMESTAMPTZ NOT NULL,
    reported_kwh NUMERIC(12,3) NOT NULL,
    corrected BOOLEAN DEFAULT FALSE,
    correction_reason TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS investments (
    investment_id SERIAL PRIMARY KEY,
    facility_id INTEGER REFERENCES facilities(facility_id),
    investment_date DATE NOT NULL,
    amount_eur NUMERIC(14,2) NOT NULL,
    category VARCHAR(50) CHECK (category IN ('renewable','storage','efficiency','flexibility')),
    status VARCHAR(50) DEFAULT 'confirmed',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE meter_readings REPLICA IDENTITY FULL;
ALTER TABLE investments REPLICA IDENTITY FULL;
ALTER TABLE facilities REPLICA IDENTITY FULL;
