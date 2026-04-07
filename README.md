# Weather Pipeline - Technical Case Study

A production-quality data engineering solution for ingesting UK weather data from the Open-Meteo API and transforming it through a three layer data warehouse architecture.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run data ingestion
python ingest.py

# 3. Run data transformation
python transform.py

# 4. Query results
sqlite3 weather.db "SELECT * FROM fct_weather_summary"
```

---

## Technology Stack

### Database: SQLite
- File based SQL database (single `weather.db` file)
- serverless
- Database schema is portable to PostgreSQL for production

### HTTP Library: requests
- Industry standard library for API calls
- Clean, intuitive interface
- Replaces urllib with professional-grade HTTP handling

### Languages & Libraries
```
Python 3.7+
- requests (HTTP operations)
- sqlite3 (database)
- json (data parsing)
- logging (operational logging)
```

---

## Architecture

### Data Flow
```
Open-Meteo API
    ↓
ingest.py (fetch & load via requests)
    ↓
raw_weather table (28 rows - raw API data)
    ↓
transform.py (SQL transformations)
    ↓
stg_weather table (28 rows - deduplicated staging)
    ↓
fct_weather_summary table (4 rows - aggregated facts)
```

### Design Principles
- **Idempotent ingestion**: INSERT OR REPLACE prevents duplicates on reruns
- **Resilient error handling**: Individual city failures don't crash pipeline
- **Clear separation**: Python for ingestion, SQL for transformation
- **Simple, readable code**: Direct iteration and explicit logic

---

## Project Structure

```
Case_study/
├── ingest.py                          # API ingestion (requests library)
├── transform.py                       # Data transformation via SQL
├── models/
│   ├── staging/stg_weather.sql        # Staging layer SQL
│   ├── marts/fct_weather_summary.sql  # Fact layer SQL
│   └── schema.yml                     # Schema documentation
├── weather.db                         # SQLite database (auto-generated)
├── requirements.txt                   # Python dependencies
└── README.md                          # This file
```

---

## Database Schema

### raw_weather (28 rows)
Source table loaded directly from Open-Meteo API
```sql
CREATE TABLE raw_weather (
    city TEXT NOT NULL,
    date DATE NOT NULL,
    temp_max_c FLOAT,
    temp_min_c FLOAT,
    precipitation_mm FLOAT,
    ingested_at TIMESTAMP NOT NULL,
    PRIMARY KEY (city, date)
)
```

### stg_weather (28 rows)
Staging table - deduplicated and cleaned
```sql
CREATE TABLE stg_weather AS
SELECT
    city,
    date,
    temp_max_c,
    temp_min_c,
    precipitation_mm,
    MAX(ingested_at) as ingested_at
FROM raw_weather
WHERE date IS NOT NULL
GROUP BY city, date
```

### fct_weather_summary (4 rows)
Fact table - aggregated metrics by city
```sql
CREATE TABLE fct_weather_summary AS
SELECT
    city,
    ROUND(AVG(temp_max_c), 2) as avg_temp_max_c,
    ROUND(AVG(temp_min_c), 2) as avg_temp_min_c,
    ROUND(SUM(precipitation_mm), 2) as total_precipitation_mm,
    COUNT(CASE WHEN precipitation_mm > 1 THEN 1 END) as rainy_days,
    (SELECT date FROM stg_weather s2 WHERE s2.city = s1.city 
     ORDER BY temp_max_c DESC LIMIT 1) as hottest_day
FROM stg_weather s1
GROUP BY city
```

---

## How to Use

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Run Ingestion
```bash
python ingest.py
```

### Step 3: Run Transformations
```bash
python transform.py
```

**Output:**


### Step 4: Query Results
```bash
# View fact table
sqlite3 weather.db "SELECT * FROM fct_weather_summary"

# View staging table
sqlite3 weather.db "SELECT * FROM stg_weather LIMIT 5"

# View raw data
sqlite3 weather.db "SELECT * FROM raw_weather LIMIT 10"
```

---

## Code Examples

### API Integration with requests

```python
import requests

# Setup parameters
params = {
    "latitude": 51.51,
    "longitude": -0.13,
    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
    "timezone": "UTC"
}

# Make API call
try:
    response = requests.get(API_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
except requests.exceptions.HTTPError as e:
    print(f"HTTP Error: {e.response.status_code}")
except requests.exceptions.Timeout as e:
    print(f"Timeout: {e}")
```

### Database Operations

```python
import sqlite3

conn = sqlite3.connect("weather.db")
cursor = conn.cursor()

# Insert data idempotently
cursor.execute("""
    INSERT OR REPLACE INTO raw_weather 
    (city, date, temp_max_c, temp_min_c, precipitation_mm, ingested_at)
    VALUES (?, ?, ?, ?, ?, ?)
""", row)

conn.commit()
conn.close()
```

### Error Handling Pattern

```python
try:
    # Operation
    response = requests.get(url, timeout=10)
    response.raise_for_status()
except requests.exceptions.ConnectionError as e:
    print(f"Connection Error: {e}")
    continue  # Skip this city, continue pipeline
except Exception as e:
    print(f"Error: {e}")
    return None
```
---

## Troubleshooting

### Database not found
```bash
# Ensure you ran ingest.py first
ls -la weather.db
```

### API request error
- Check internet connection
- Verify Open-Meteo API is accessible: `curl https://api.open-meteo.com/v1/forecast`
- Check timeout settings (default 10 seconds)

### Transform errors
- Ensure raw_weather table exists: `sqlite3 weather.db ".tables"`
- Check data types: `sqlite3 weather.db ".schema raw_weather"`

---

## Task 3: Testing & Reliability Answers

### Question 1: dbt Tests for stg_weather and fct_weather_summary

Tests are defined in `models/schema.yml` and include:

**stg_weather tests:**
- `not_null` on city and date columns (every weather record must have city and date)
- `unique` on (city, date) composite key (no duplicate city/date combinations after deduplication)
- Column order validation (ensures schema consistency)

**fct_weather_summary tests:**
- `not_null` on all aggregation columns (avg_temp_max_c, avg_temp_min_c, total_precipitation_mm, rainy_days, hottest_day)
- Exactly 4 rows (one per city: London, Manchester, Edinburgh, Bristol)
- Temperature logic validation (avg_temp_max_c >= avg_temp_min_c for each city)

### Question 2: Scheduling Daily Ingestion at 6 AM

**Approach: Using Airflow (Recommended for Production)**

1. **Create DAG** with schedule interval `"0 6 * * *"` (cron format for 6 AM daily)
2. **Tasks structure:**
   - Task 1: Run `python ingest.py` to fetch latest API data
   - Task 2: Run `python transform.py` to refresh staging and fact tables
   - Task 3: Data quality checks (row counts, schema validation)
   - Task 4: Alert on-call team if failures occur

3. **Benefits:** Centralized scheduling, dependency management, monitoring, retry logic, data lineage tracking

**Alternative approaches:**
- **Linux cron:** Simple but requires server maintenance: `0 6 * * * /usr/bin/python3 /path/ingest.py`
- **AWS Lambda + CloudWatch:** Serverless, pay-per-execution, good for small jobs
- **Docker + Kubernetes CronJob:** Container-based, scales with infrastructure

### Question 3: Data Quality Issue & Prevention

**Identified Issue: Temperature Reversal (temp_min > temp_max)**

**Why it matters:** Invalid data can corrupt analytics. A day with min > max is physically impossible.

**Detection approaches:**
1. **In staging model (stg_weather):**
   ```sql
   WHERE temp_max_c >= temp_min_c  -- Filter invalid records
   ```

2. **In dbt tests:**
   ```yaml
   - name: fct_weather_summary
     tests:
       - custom_sql_test:
           sql: "SELECT * FROM {{ this }} WHERE avg_temp_max_c < avg_temp_min_c"
           # This query should return 0 rows if data is valid
   ```

3. **Programmatic validation in ingest.py:**
   ```python
   if temps_max[i] < temps_min[i]:
       print(f"Warning: Invalid temps for {city} on {dates[i]}")
       skip_record = True  # Don't insert invalid data
   ```

**Prevention:**
- Add data quality checks in the ingestion layer before database load
- Document expected ranges and constraints
- Monitor ingestion logs for anomalies
- Set up alerting for unusual patterns (e.g., all temps in unexpected range)

---

## Summary

This case study demonstrates:
- API integration with requests library
- Data ingestion and loading
- SQL transformations and aggregations
- Three-layer data warehouse pattern
- Error handling and resilience
- Code clarity and maintainability

