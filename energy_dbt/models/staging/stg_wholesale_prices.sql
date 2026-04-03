-- Real SMARD wholesale electricity prices, aggregated to monthly average
SELECT
    DATE_TRUNC('month', price_timestamp) AS price_month,
    ROUND(AVG(price_eur_per_mwh), 2) AS avg_price_eur_per_mwh,
    ROUND(AVG(price_eur_per_kwh), 6) AS avg_price_eur_per_kwh,
    ROUND(MIN(price_eur_per_mwh), 2) AS min_price_eur_per_mwh,
    ROUND(MAX(price_eur_per_mwh), 2) AS max_price_eur_per_mwh,
    COUNT(*) AS price_observations
FROM {{ source('energy_compliance', 'smard_wholesale_prices') }}
GROUP BY DATE_TRUNC('month', price_timestamp)
