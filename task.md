Junior Data Engineer — Technical Case Study
Estimated time: 25 – 30 minutes
Tools: Python · dbt · PostgreSQL (or SQLite)

Background
You are a data engineer at a company that wants to track weather conditions across UK cities.
Your task is to build a small data pipeline that ingests data from a public REST API, loads it into a
database, and transforms it into a clean analytical model using dbt.

You will be working with the Open-Meteo API — a free, no-auth-required weather API.

API endpoint (example):

Documentation: https://open-meteo.com/en/docs

Cities to Ingest
City Latitude Longitude
London 51.51 -0.
Manchester 53.48 -2.
Edinburgh 55.95 -3.
Bristol 51.45 -2.
Tasks
Task 1 — Python Ingestion Script ( 10 – 12 min)
Write a Python script (ingest.py) that:

Loops over the four cities above and calls the Open-Meteo API for each, requesting the
following daily fields for the next 7 days:
temperature_2m_max
https://api.open-meteo.com/v1/forecast?
latitude=51.5&longitude=-0.12&daily=temperature_2m_max,temperature_2m_min,precipitat
temperature_2m_min
precipitation_sum
Parses the JSON response and flattens it into rows (one row per city per day).
Loads the raw rows into a database table called raw_weather. The table should have at
least these columns:
Column Type
city TEXT
date DATE
temp_max_c FLOAT
temp_min_c FLOAT
precipitation_mm FLOAT
ingested_at TIMESTAMP
Handles the following error cases gracefully:
The API returns a non- 200 status code
A field is missing or null in the response
The database insert fails
Note: You may use any database you're comfortable with (PostgreSQL, SQLite, DuckDB). If
using SQLite, you can create the DB file locally. If using PostgreSQL, assume a local instance
is running.
Task 2 — dbt Transformation ( 10 – 12 min)
Using dbt (with the same database as Task 1 ), create the following two models:

Model 1: stg_weather (staging)
A cleaned, typed staging model on top of raw_weather that:

Casts columns to the correct data types
Renames columns if needed for clarity
Filters out any rows where date is null
Deduplicates rows if the same city + date combination appears more than once (keep
the most recently ingested record)
Model 2: fct_weather_summary (mart/fact)
An analytical model built on stg_weather that produces one row per city with the following
fields:

Column Description
city City^ name
avg_temp_max_c Average^ daily^ high^ temperature^ across^ all^ dates
avg_temp_min_c Average^ daily^ low^ temperature^ across^ all^ dates
total_precipitation_mm Total^ precipitation^ across^ all^ dates
rainy_days Count^ of^ days^ where^ precipitation^ >^1 mm
hottest_day The date with the highest temp_max_c
Tip: You don't need a full dbt project set up — showing the SQL in the model files with
correct {{ ref() }} usage and a brief schema.yml is sufficient.
Task 3 — Testing & Reliability ( 5 min)
Answer the following questions either in writing or in code — whichever you prefer:

What dbt tests would you add to stg_weather and fct_weather_summary? Write them
into a schema.yml file (or describe them clearly).
The ingestion script currently runs manually. If this needed to run daily at 6 am , how would
you schedule it? Describe the approach you would take (you may reference Airflow, cron, or
any tool you know).
Identify one potential data quality issue in this pipeline and explain how you would detect
or prevent it.
Deliverables
ingest.py — Python ingestion script
models/staging/stg_weather.sql
models/marts/fct_weather_summary.sql
models/schema.yml (with at least a few dbt tests)
Brief written answers to Task 3 questions 2 & 3 (a few sentences each is fine)
Evaluation Criteria
Area What we're looking for
Pipeline design Clean separation of ingestion and transformation; sensible table structure;
idempotency considered
SQL / transformations Correct aggregations; deduplication logic; clear, readable SQL
Error handling &
testing
API errors caught; nulls handled; meaningful dbt tests defined
Code quality Readable, reasonably structured code — not necessarily production-perfect,
but coherent
Notes for Candidates
You do not need to deploy anything — working local code is fine.
You are free to use any Python libraries you like (requests, pandas, psycopg2, duckdb,
etc.).
If you run out of time, prioritise Tasks 1 and 2 and leave written notes for anything
incomplete.
There is no single "correct" answer — we are interested in how you approach the problem
and the decisions you make.


### View all fact metrics
```bash
sqlite3 weather.db << EOF
.mode column
.headers on
SELECT * FROM fct_weather_summary;
EOF
```

### Find coldest city
```bash
sqlite3 weather.db "SELECT city, avg_temp_min_c FROM fct_weather_summary ORDER BY avg_temp_min_c ASC LIMIT 1"
```

### Find wettest city
```bash
sqlite3 weather.db "SELECT city, total_precipitation_mm FROM fct_weather_summary ORDER BY total_precipitation_mm DESC LIMIT 1"
```

### View staging sample
```bash
sqlite3 weather.db "SELECT city, date, temp_max_c, precipitation_mm FROM stg_weather WHERE city='London' ORDER BY date"
```

---