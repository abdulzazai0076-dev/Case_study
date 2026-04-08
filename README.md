# Weather Pipeline - Technical Case Study

A data engineering pipeline that ingests UK weather data from the Open-Meteo API, applies pandas transformations, and loads to a SQLite warehouse.

**Stack:** Python 3.7+ | pandas | requests | SQLite

---

## Quick Start

```bash
pip install -r requirements.txt
python ingest.py
python transform.py
sqlite3 weather.db "SELECT * FROM fct_weather_summary"
```

---

## Architecture

### Data Flow
```
Open-Meteo API → ingest.py → raw_weather (28 rows)
                                    ↓
                           transform.py (pandas)
                                    ↓
stg_weather (28 rows, deduplicated) → fct_weather_summary (4 rows, aggregated)
```

### Design Choices
- **pandas DataFrames**: Efficient for transformations, familiar for analysis
- **requests library**: Clean HTTP handling, industry standard
- **SQLite**: Lightweight, file-based, direct pandas integration
- **Idempotent operations**: `to_sql(if_exists='replace')` for safe reruns
- **Error resilience**: Individual city failures don't crash pipeline

---

## Project Structure

```
Case_study/
├── ingest.py                    # API ingestion (28 rows → raw_weather)
├── transform.py                 # Pandas transformations (dedup, aggregate)
├── test_alignment.py            # Test suite (5/5 passing)
├── models/
│   ├── schema.yml               # Schema documentation & tests
│   ├── staging/stg_weather.sql  # Reference SQL for staging logic
│   └── marts/fct_weather_summary.sql  # Reference SQL for facts
├── weather.db                   # SQLite (auto-generated, excluded from git)
└── requirements.txt
```

---

## Database Schema

**raw_weather** (28 rows) - Raw API data
| city | date | temp_max_c | temp_min_c | precipitation_mm | ingested_at |

**stg_weather** (28 rows) - Deduplicated staging  
Same columns as raw, with duplicates on (city, date) removed, keeping most recent.

**fct_weather_summary** (4 rows) - City aggregates
| city | avg_temp_max_c | avg_temp_min_c | total_precipitation_mm | rainy_days | hottest_day |

---

## Task 3: Testing & Reliability

### Q1: dbt Tests for stg_weather and fct_weather_summary

**stg_weather:**
- `not_null` on city, date (every record must have identifying info)
- `unique` on (city, date) composite key (no duplicates post-dedup)

**fct_weather_summary:**
- `not_null` on all aggregation columns
- Row count = 4 (one per city: London, Manchester, Edinburgh, Bristol)
- `avg_temp_max_c >= avg_temp_min_c` (temperature logic)

### Q2: Scheduling Daily at 6 AM

**Recommended: Apache Airflow**
- DAG with `schedule_interval="0 6 * * *"` (cron: 6 AM daily)
- Task 1: `python ingest.py` (fetch API)
- Task 2: `python transform.py` (refresh models)
- Task 3: Data quality checks (row counts, schema validation)
- Benefits: Centralized scheduling, monitoring, retry logic, alerting

**Alternatives:**
- Linux cron: `0 6 * * * python /path/ingest.py` (simple but manual)
- AWS Lambda + CloudWatch: Serverless, pay-per-execution
- Kubernetes CronJob: Container-based, infrastructure-agnostic

### Q3: Data Quality Issue & Prevention

**Issue Identified: Temperature Reversal (temp_min > temp_max)**

Why it matters: Invalid data corrupts analytics. Min > Max is physically impossible.

**Detection (in stg_weather):**
```sql
WHERE temp_max_c >= temp_min_c  -- Filter invalid records
```

**Prevention:**
- Add validation in ingest.py before database load
- Monitor ingestion logs for anomalies
- Set up alerting for unusual patterns
- Document expected ranges and constraints

---

## Implementation Notes

- **Pandas vs SQL**: Transformations use pandas (transform.py) for familiarity; reference SQL in models/ shows equivalent dbt approach
- **Error Handling**: HTTPError, Timeout, ConnectionError all handled to skip single cities without crashing
- **Database Generation**: Run ingest.py + transform.py to regenerate weather.db (not tracked in git)
- **Testing**: Run `python test_alignment.py` to verify all transformations (5 tests, all passing)

