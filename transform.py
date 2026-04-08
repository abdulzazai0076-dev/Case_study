"""
Data transformation - creates staging and fact models using pandas
Performs deduplication, aggregation, and data quality checks
"""

import pandas as pd
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "weather.db"


def create_staging_model(df):
    """
    Create staging model from raw data
    - Deduplicate on (city, date)
    - Filter out null dates
    - Keep most recent ingested_at
    """
    logger.info("Creating stg_weather staging model...")
    
    # Filter out rows with null dates
    df_clean = df[df['date'].notna()].copy()
    
    # Deduplicate: keep last ingested record per (city, date)
    df_staging = df_clean.sort_values('ingested_at').drop_duplicates(
        subset=['city', 'date'],
        keep='last'
    )
    
    logger.info(f"Staging model created: {len(df_staging)} rows")
    return df_staging


def create_fact_model(df_staging):
    """
    Create fact model from staging data
    - One row per city
    - Calculate aggregations:
      * avg_temp_max_c: Average high temperature
      * avg_temp_min_c: Average low temperature
      * total_precipitation_mm: Sum of precipitation
      * rainy_days: Count of days with precipitation > 1mm
      * hottest_day: Date with highest temperature
    """
    logger.info("Creating fct_weather_summary fact model...")
    
    # Group by city and calculate aggregations
    df_fact = df_staging.groupby('city').agg({
        'temp_max_c': 'mean',
        'temp_min_c': 'mean',
        'precipitation_mm': 'sum'
    }).round(2).reset_index()
    
    # Rename columns
    df_fact.columns = ['city', 'avg_temp_max_c', 'avg_temp_min_c', 'total_precipitation_mm']
    
    # Calculate rainy days (precipitation > 1mm per city)
    rainy_days = df_staging[df_staging['precipitation_mm'] > 1].groupby('city').size()
    df_fact['rainy_days'] = df_fact['city'].map(rainy_days).fillna(0).astype(int)
    
    # Find hottest day per city
    hottest_days = df_staging.loc[
        df_staging.groupby('city')['temp_max_c'].idxmax()
    ][['city', 'date']].copy()
    hottest_days.columns = ['city', 'hottest_day']
    
    # Merge hottest_day into fact table
    df_fact = df_fact.merge(hottest_days, on='city', how='left')
    
    # Sort by city
    df_fact = df_fact.sort_values('city').reset_index(drop=True)
    
    logger.info(f"Fact model created: {len(df_fact)} rows")
    return df_fact


def save_to_database(df_staging, df_fact):
    """Save staging and fact models to SQLite database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Save staging model
        df_staging.to_sql('stg_weather', conn, if_exists='replace', index=False)
        
        # Save fact model
        df_fact.to_sql('fct_weather_summary', conn, if_exists='replace', index=False)
        
        conn.close()
        logger.info("Models saved to database")
        
    except Exception as e:
        logger.error(f"Error saving models: {e}")
        raise


def print_results(df_staging, df_fact):
    """Print transformation results"""
    logger.info("\n" + "=" * 60)
    logger.info("STAGING MODEL (stg_weather):")
    logger.info("=" * 60)
    logger.info(f"Rows: {len(df_staging)}")
    logger.info(f"Columns: {', '.join(df_staging.columns)}")
    logger.info(f"Sample:\n{df_staging.head()}\n")
    
    logger.info("=" * 60)
    logger.info("FACT MODEL (fct_weather_summary):")
    logger.info("=" * 60)
    logger.info(f"Rows: {len(df_fact)}")
    logger.info(f"Columns: {', '.join(df_fact.columns)}")
    logger.info(f"Results:\n{df_fact}\n")


def execute_transformations():
    """Execute staging and fact model transformations"""
    try:
        # Load raw data from database
        conn = sqlite3.connect(DB_PATH)
        df_raw = pd.read_sql_query("SELECT * FROM raw_weather", conn)
        conn.close()
        
        logger.info(f"Loaded {len(df_raw)} raw records\n")
        
        # Create staging model
        df_staging = create_staging_model(df_raw)
        
        # Create fact model
        df_fact = create_fact_model(df_staging)
        
        # Save to database
        save_to_database(df_staging, df_fact)
        
        # Print results
        print_results(df_staging, df_fact)
        
        logger.info("Transformations complete!")
        
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise


if __name__ == "__main__":
    execute_transformations()
