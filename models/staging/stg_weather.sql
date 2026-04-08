/*
REFERENCE IMPLEMENTATION: Staging model concept for dbt workflow.
Actual implementation uses pandas in transform.py for this case study.

Conceptual SQL - cleaned, typed, deduplicated raw weather data.
- Filters null dates
- Dedupes on (city, date), keeps most recent ingested_at
*/

WITH raw AS (
    SELECT
        city,
        date::DATE as date,
        temp_max_c::FLOAT as temp_max_c,
        temp_min_c::FLOAT as temp_min_c,
        precipitation_mm::FLOAT as precipitation_mm,
        ingested_at::TIMESTAMP as ingested_at
    FROM {{ source('raw', 'raw_weather') }}
    WHERE date IS NOT NULL
),

deduplicated AS (
    SELECT
        city,
        date,
        temp_max_c,
        temp_min_c,
        precipitation_mm,
        ingested_at,
        ROW_NUMBER() OVER (PARTITION BY city, date ORDER BY ingested_at DESC) as rn
    FROM raw
)

SELECT
    city,
    date,
    temp_max_c,
    temp_min_c,
    precipitation_mm,
    ingested_at
FROM deduplicated
WHERE rn = 1
