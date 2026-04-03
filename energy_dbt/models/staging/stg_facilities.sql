SELECT
    facility_id,
    name,
    sector,
    region,
    subsidy_eligible
FROM {{ source('energy_compliance', 'dim_facilities') }}
