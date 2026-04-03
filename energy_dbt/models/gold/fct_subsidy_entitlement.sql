-- CISAF subsidy entitlement per facility per month
-- Uses REAL SMARD wholesale prices instead of hardcoded values
-- Formula: 50% of consumption at (actual_market_price - target_price)
-- Target price: 0.05 EUR/kWh (5 cents, per CISAF regulation)
-- Market price: actual monthly average from SMARD day-ahead auction
WITH monthly AS (
    SELECT *
    FROM {{ ref('fct_monthly_consumption') }}
),
facilities AS (
    SELECT *
    FROM {{ ref('stg_facilities') }}
),
prices AS (
    SELECT *
    FROM {{ ref('stg_wholesale_prices') }}
)
SELECT
    m.facility_id,
    m.facility_name,
    m.sector,
    m.consumption_month,
    m.total_kwh,
    f.subsidy_eligible,
    -- Real market price for this month
    p.avg_price_eur_per_kwh AS market_price_eur_per_kwh,
    p.avg_price_eur_per_mwh AS market_price_eur_per_mwh,
    p.min_price_eur_per_mwh,
    p.max_price_eur_per_mwh,
    -- CISAF target price
    0.05 AS target_price_eur_per_kwh,
    -- Price gap (what the subsidy covers)
    ROUND(GREATEST(p.avg_price_eur_per_kwh - 0.05, 0), 6) AS price_gap_eur_per_kwh,
    -- Subsidy calculation with real prices
    CASE
        WHEN f.subsidy_eligible THEN m.total_kwh * 0.5
        ELSE 0
    END AS eligible_kwh,
    CASE
        WHEN f.subsidy_eligible THEN
            ROUND(m.total_kwh * 0.5 * GREATEST(p.avg_price_eur_per_kwh - 0.05, 0), 2)
        ELSE 0
    END AS subsidy_entitlement_eur,
    -- Running total for the year
    CASE
        WHEN f.subsidy_eligible THEN
            ROUND(SUM(m.total_kwh * 0.5 * GREATEST(p.avg_price_eur_per_kwh - 0.05, 0)) OVER (
                PARTITION BY m.facility_id
                ORDER BY m.consumption_month
                ROWS UNBOUNDED PRECEDING
            ), 2)
        ELSE 0
    END AS cumulative_subsidy_eur
FROM monthly m
JOIN facilities f ON m.facility_id = f.facility_id
LEFT JOIN prices p ON m.consumption_month = p.price_month
