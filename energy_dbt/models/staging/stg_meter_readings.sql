-- Current meter readings only (latest corrected values)
SELECT
    reading_id,
    facility_id,
    reading_timestamp,
    reported_kwh,
    corrected,
    correction_reason,
    valid_from,
    valid_to,
    is_current
FROM {{ source('energy_compliance', 'silver_meter_readings') }}
WHERE is_current = true
