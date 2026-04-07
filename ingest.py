"""
Weather data ingestion script
Pulls data from Open-Meteo API and loads into SQLite database
"""

import sqlite3
import requests
import json
from datetime import datetime

# Configuration
API_URL = "https://api.open-meteo.com/v1/forecast"
DB_PATH = "weather.db"

CITIES = [
    {"name": "London", "lat": 51.51, "lon": -0.13},
    {"name": "Manchester", "lat": 53.48, "lon": -2.24},
    {"name": "Edinburgh", "lat": 55.95, "lon": -3.19},
    {"name": "Bristol", "lat": 51.45, "lon": -2.58},
]


def init_database():
    """Create database and raw_weather table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_weather (
            city TEXT NOT NULL,
            date DATE NOT NULL,
            temp_max_c FLOAT,
            temp_min_c FLOAT,
            precipitation_mm FLOAT,
            ingested_at TIMESTAMP NOT NULL,
            PRIMARY KEY (city, date)
        )
    """)
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")


def get_weather_data(city):
    """Fetch weather data from Open-Meteo API for a single city"""
    try:
        # Build URL with parameters
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": "UTC"
        }
        
        # Make API call
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Extract daily data
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        temps_max = daily.get("temperature_2m_max", [])
        temps_min = daily.get("temperature_2m_min", [])
        precip = daily.get("precipitation_sum", [])
        
        if not dates:
            print(f"Warning: No date data for {city['name']}")
            return None
        
        # Create rows
        rows = []
        for i in range(len(dates)):
            row = [
                city["name"],
                dates[i],
                temps_max[i] if i < len(temps_max) else None,
                temps_min[i] if i < len(temps_min) else None,
                precip[i] if i < len(precip) else None,
                datetime.utcnow().isoformat()
            ]
            rows.append(row)
        
        print(f"Fetched {len(rows)} records for {city['name']}")
        return rows
    
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error for {city['name']}: {e.response.status_code}")
        return None
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error for {city['name']}: {e}")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout Error for {city['name']}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON Error for {city['name']}: {e}")
        return None
    except Exception as e:
        print(f"Error for {city['name']}: {e}")
        return None


def insert_data(rows):
    """Insert rows into database"""
    if not rows:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    inserted = 0
    try:
        for row in rows:
            cursor.execute("""
                INSERT OR REPLACE INTO raw_weather 
                (city, date, temp_max_c, temp_min_c, precipitation_mm, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, row)
            inserted += 1
        
        conn.commit()
        print(f"Inserted {inserted} records")
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
        return 0
    
    finally:
        conn.close()
    
    return inserted


def main():
    """Main ingestion process"""
    print("Starting weather data ingestion...")
    print()
    
    init_database()
    total = 0
    
    for city in CITIES:
        print(f"Processing {city['name']}...")
        
        # Get data from API
        rows = get_weather_data(city)
        
        if rows:
            # Insert into database
            count = insert_data(rows)
            total += count
        else:
            print(f"Skipped {city['name']} - no data")
        
        print()
    
    print(f"Ingestion complete: {total} total records loaded")


if __name__ == "__main__":
    main()
