-- Investment compliance tracking per facility
-- CISAF requires 50% of subsidy to be reinvested in decarbonization within 48 months
WITH facility_subsidies AS (
    SELECT
        facility_id,
        facility_name,
        sector,
        SUM(subsidy_entitlement_eur) AS total_subsidy_eur,
        ROUND(SUM(subsidy_entitlement_eur) * 0.5, 2) AS reinvestment_obligation_eur
    FROM {{ ref('fct_subsidy_entitlement') }}
    WHERE subsidy_eligible = true
    GROUP BY facility_id, facility_name, sector
),
facility_investments AS (
    SELECT
        i.facility_id,
        COUNT(*) AS total_investments,
        SUM(CASE WHEN i.status = 'confirmed' THEN i.amount_eur ELSE 0 END) AS confirmed_investment_eur,
        SUM(CASE WHEN i.status = 'pending' THEN i.amount_eur ELSE 0 END) AS pending_investment_eur,
        SUM(i.amount_eur) AS total_investment_eur,
        -- By category
        SUM(CASE WHEN i.category = 'renewable' AND i.status = 'confirmed' THEN i.amount_eur ELSE 0 END) AS renewable_eur,
        SUM(CASE WHEN i.category = 'storage' AND i.status = 'confirmed' THEN i.amount_eur ELSE 0 END) AS storage_eur,
        SUM(CASE WHEN i.category = 'efficiency' AND i.status = 'confirmed' THEN i.amount_eur ELSE 0 END) AS efficiency_eur,
        SUM(CASE WHEN i.category = 'flexibility' AND i.status = 'confirmed' THEN i.amount_eur ELSE 0 END) AS flexibility_eur
    FROM {{ ref('stg_investments') }} i
    GROUP BY i.facility_id
)
SELECT
    s.facility_id,
    s.facility_name,
    s.sector,
    s.total_subsidy_eur,
    s.reinvestment_obligation_eur,
    COALESCE(inv.confirmed_investment_eur, 0) AS confirmed_investment_eur,
    COALESCE(inv.pending_investment_eur, 0) AS pending_investment_eur,
    COALESCE(inv.total_investment_eur, 0) AS total_investment_eur,
    -- Compliance calculation
    ROUND(COALESCE(inv.confirmed_investment_eur, 0) / NULLIF(s.reinvestment_obligation_eur, 0) * 100, 1) AS compliance_pct,
    CASE
        WHEN COALESCE(inv.confirmed_investment_eur, 0) >= s.reinvestment_obligation_eur THEN 'COMPLIANT'
        WHEN COALESCE(inv.total_investment_eur, 0) >= s.reinvestment_obligation_eur THEN 'ON_TRACK'
        ELSE 'AT_RISK'
    END AS compliance_status,
    -- Category breakdown
    COALESCE(inv.renewable_eur, 0) AS renewable_eur,
    COALESCE(inv.storage_eur, 0) AS storage_eur,
    COALESCE(inv.efficiency_eur, 0) AS efficiency_eur,
    COALESCE(inv.flexibility_eur, 0) AS flexibility_eur,
    -- Flexibility bonus: 10% bonus if 80%+ of investment goes to flexibility
    CASE
        WHEN COALESCE(inv.flexibility_eur, 0) >= COALESCE(inv.confirmed_investment_eur, 0) * 0.8
        THEN true
        ELSE false
    END AS flexibility_bonus_eligible
FROM facility_subsidies s
LEFT JOIN facility_investments inv ON s.facility_id = inv.facility_id
