/*
REFERENCE IMPLEMENTATION: Fact table concept for dbt workflow.
Actual implementation uses pandas in transform.py for this case study.

Conceptual SQL - analytical summary of weather data per city.
*/

WITH daily_stats AS (
    SELECT
        city,
        date,
        temp_max_c,
        temp_min_c,
        precipitation_mm,
        CASE WHEN precipitation_mm > 1 THEN 1 ELSE 0 END as is_rainy_day
    FROM {{ ref('stg_weather') }}
),

city_aggregates AS (
    SELECT
        city,
        ROUND(AVG(temp_max_c), 2) as avg_temp_max_c,
        ROUND(AVG(temp_min_c), 2) as avg_temp_min_c,
        ROUND(SUM(precipitation_mm), 2) as total_precipitation_mm,
        SUM(is_rainy_day) as rainy_days,
        MAX(CASE WHEN temp_max_c = (SELECT MAX(temp_max_c) FROM daily_stats ds2 WHERE ds2.city = daily_stats.city) THEN date END) as hottest_day
    FROM daily_stats
    GROUP BY city
)

SELECT
    city,
    avg_temp_max_c,
    avg_temp_min_c,
    total_precipitation_mm,
    rainy_days,
    hottest_day
FROM city_aggregates
ORDER BY city
