-- Monthly electricity consumption per facility
-- Base table for subsidy entitlement calculation
SELECT
    mr.facility_id,
    f.name AS facility_name,
    f.sector,
    f.region,
    DATE_TRUNC('month', mr.reading_timestamp) AS consumption_month,
    COUNT(*) AS reading_count,
    SUM(mr.reported_kwh) AS total_kwh,
    AVG(mr.reported_kwh) AS avg_kwh_per_reading,
    SUM(CASE WHEN mr.corrected THEN 1 ELSE 0 END) AS corrected_readings,
    ROUND(SUM(CASE WHEN mr.corrected THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS correction_rate_pct
FROM {{ ref('stg_meter_readings') }} mr
JOIN {{ ref('stg_facilities') }} f ON mr.facility_id = f.facility_id
GROUP BY
    mr.facility_id,
    f.name,
    f.sector,
    f.region,
    DATE_TRUNC('month', mr.reading_timestamp)
