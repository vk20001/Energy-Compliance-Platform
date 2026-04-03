-- Current investment records only (latest status)
SELECT
    investment_id,
    facility_id,
    investment_date,
    amount_eur,
    category,
    status,
    valid_from,
    valid_to,
    is_current
FROM {{ source('energy_compliance', 'silver_investments') }}
WHERE is_current = true
