# Quick Start Guide

This project requires **only Python 3.7+** with no external dependencies.

## Setup (30 seconds)

```bash
# 1. Clone or download the repository
cd Case_study

# 2. Run the ingestion pipeline
python3 ingest.py

# 3. Run the transformations
python3 transform.py

# 4. Verify everything works
python3 test_alignment.py
```

## What You'll See

✅ **Ingestion**: Fetches weather data from 4 UK cities for 7 days
- Creates `raw_weather` table with 28 rows

✅ **Transformation**: Applies staging and fact models
- Creates `stg_weather` (deduplicated, 28 rows)
- Creates `fct_weather_summary` (aggregated, 4 rows - one per city)

✅ **Tests**: Comprehensive validation suite
- 5 tests covering all task requirements
- Verifies data integrity and SQL correctness

## Database

SQLite file: `weather.db` (portable, no server needed)

```bash
# Query the database directly:
sqlite3 weather.db "SELECT * FROM fct_weather_summary;"
```

## Project Structure

```
.
├── ingest.py                          # API ingestion (requests library)
├── transform.py                       # SQL transformations
├── models/
│   ├── staging/stg_weather.sql       # Staging model (deduplication)
│   ├── marts/fct_weather_summary.sql # Fact model (aggregations)
│   └── schema.yml                    # dbt tests and documentation
├── weather.db                         # SQLite database
├── test_alignment.py                  # Test suite (verify all tasks)
├── README.md                          # Complete documentation
└── requirements.txt                   # Dependencies (only built-ins)
```

## Key Features

- **Zero External Dependencies**: Only Python standard library (sqlite3, requests, json, logging)
- **Professional Error Handling**: API failures, database errors all caught gracefully
- **dbt-Style Modeling**: Uses dbt semantics ({{ ref() }}, {{ source() }})
- **Data Quality Tests**: not_null, unique, deduplication validation
- **Interview-Ready**: Clean code, clear logic, easy to explain

## Talking Points

1. Why requests library? → Industry standard for API calls
2. Why SQLite? → Local development, portable schema to PostgreSQL
3. How is it idempotent? → INSERT OR REPLACE on (city, date)
4. Error handling? → Each city independent, graceful failures
5. Production improvements? → Airflow scheduling, PostgreSQL backend

## Questions?

See `README.md` for complete documentation and Task 3 answers.
