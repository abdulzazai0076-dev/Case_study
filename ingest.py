"""
Weather data ingestion script
Fetches data from Open-Meteo API and loads into SQLite database using pandas
"""

import pandas as pd
import sqlite3
import requests
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


def get_weather_data(city):
    """Fetch weather data from Open-Meteo API for a single city"""
    try:
        # Build API parameters
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": "UTC"
        }
        
        # Make API request
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
            print(f"  Warning: No date data for {city['name']}")
            return None
        
        # Create DataFrame from API response
        df = pd.DataFrame({
            "city": city["name"],
            "date": dates,
            "temp_max_c": temps_max,
            "temp_min_c": temps_min,
            "precipitation_mm": precip,
            "ingested_at": datetime.utcnow().isoformat()
        })
        
        print(f"  Fetched {len(df)} records")
        return df
    
    except requests.exceptions.HTTPError as e:
        print(f"  HTTP Error: {e.response.status_code}")
        return None
    except requests.exceptions.ConnectionError:
        print(f"  Connection Error")
        return None
    except requests.exceptions.Timeout:
        print(f"  Timeout Error")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


def ingest_all_cities():
    """Fetch and combine data from all cities"""
    print("Starting weather data ingestion...\n")
    
    all_data = []
    
    for city in CITIES:
        print(f"Processing {city['name']}...")
        df = get_weather_data(city)
        
        if df is not None:
            all_data.append(df)
        else:
            print(f"  Skipped {city['name']} - no data")
        print()
    
    if not all_data:
        print("No data collected from any city")
        return None
    
    # Combine all city data into single DataFrame
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"Total records fetched: {len(combined_df)}\n")
    
    return combined_df


def load_to_database(df):
    """Load DataFrame into SQLite database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Write DataFrame to database
        # if_exists='replace' ensures idempotency (reingest same data)
        df.to_sql("raw_weather", conn, if_exists="replace", index=False)
        
        conn.close()
        print(f"Successfully loaded {len(df)} records to {DB_PATH}")
        
        return len(df)
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return 0
    except Exception as e:
        print(f"Error loading data: {e}")
        return 0


def main():
    """Main ingestion process"""
    # Fetch data from all cities
    df = ingest_all_cities()
    
    if df is None:
        print("Ingestion failed - no data collected")
        return
    
    # Load into database
    count = load_to_database(df)
    print(f"Ingestion complete: {count} records loaded")


if __name__ == "__main__":
    main()
