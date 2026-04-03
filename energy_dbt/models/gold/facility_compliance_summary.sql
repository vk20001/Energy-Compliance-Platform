-- Single row per facility: everything a compliance officer needs
SELECT
    f.facility_id,
    f.name AS facility_name,
    f.sector,
    f.region,
    f.subsidy_eligible,
    -- Consumption
    COALESCE(mc.total_kwh, 0) AS total_kwh_ytd,
    COALESCE(mc.months_active, 0) AS months_with_readings,
    COALESCE(mc.avg_monthly_kwh, 0) AS avg_monthly_kwh,
    -- Subsidy
    COALESCE(se.total_subsidy_eur, 0) AS total_subsidy_eur,
    -- Investment compliance
    COALESCE(ic.confirmed_investment_eur, 0) AS confirmed_investment_eur,
    COALESCE(ic.reinvestment_obligation_eur, 0) AS reinvestment_obligation_eur,
    COALESCE(ic.compliance_pct, 0) AS compliance_pct,
    COALESCE(ic.compliance_status, 'N/A') AS compliance_status,
    COALESCE(ic.flexibility_bonus_eligible, false) AS flexibility_bonus_eligible
FROM {{ ref('stg_facilities') }} f
LEFT JOIN (
    SELECT
        facility_id,
        SUM(total_kwh) AS total_kwh,
        COUNT(DISTINCT consumption_month) AS months_active,
        AVG(total_kwh) AS avg_monthly_kwh
    FROM {{ ref('fct_monthly_consumption') }}
    GROUP BY facility_id
) mc ON f.facility_id = mc.facility_id
LEFT JOIN (
    SELECT
        facility_id,
        SUM(subsidy_entitlement_eur) AS total_subsidy_eur
    FROM {{ ref('fct_subsidy_entitlement') }}
    GROUP BY facility_id
) se ON f.facility_id = se.facility_id
LEFT JOIN {{ ref('fct_investment_compliance') }} ic ON f.facility_id = ic.facility_id
