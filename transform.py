"""
Direct transformation execution - creates staging and mart models using SQL.
This mimics what dbt would do, executing the SQL from our model files.
"""

import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "weather.db"


def execute_transformations():
    """Execute staging and mart models directly."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 1. Create staging model (stg_weather)
        logger.info("Creating stg_weather staging model...")
        cursor.execute("""
            DROP TABLE IF EXISTS stg_weather
        """)
        
        # SQLite doesn't support ROW_NUMBER in CTEs with CREATE, so do it in steps
        cursor.execute("""
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
        """)
        
        stg_count = cursor.execute("SELECT COUNT(*) FROM stg_weather").fetchone()[0]
        logger.info(f"Staging model created: {stg_count} rows")
        
        # 2. Create fact model (fct_weather_summary)
        logger.info("Creating fct_weather_summary mart model...")
        cursor.execute("""
            DROP TABLE IF EXISTS fct_weather_summary
        """)
        
        cursor.execute("""
            CREATE TABLE fct_weather_summary AS
            SELECT
                city,
                ROUND(AVG(CAST(temp_max_c AS FLOAT)), 2) as avg_temp_max_c,
                ROUND(AVG(CAST(temp_min_c AS FLOAT)), 2) as avg_temp_min_c,
                ROUND(SUM(CAST(precipitation_mm AS FLOAT)), 2) as total_precipitation_mm,
                COUNT(CASE WHEN CAST(precipitation_mm AS FLOAT) > 1 THEN 1 END) as rainy_days,
                (SELECT date FROM stg_weather s2 WHERE s2.city = s1.city 
                 ORDER BY CAST(temp_max_c AS FLOAT) DESC LIMIT 1) as hottest_day
            FROM stg_weather s1
            GROUP BY city
            ORDER BY city
        """)
        
        fact_count = cursor.execute("SELECT COUNT(*) FROM fct_weather_summary").fetchone()[0]
        logger.info(f"Fact model created: {fact_count} rows")
        
        conn.commit()
        
        # Print results
        logger.info("\n" + "=" * 60)
        logger.info("STAGING MODEL (stg_weather):")
        logger.info("=" * 60)
        cursor.execute("SELECT * FROM stg_weather LIMIT 5")
        cols = [desc[0] for desc in cursor.description]
        logger.info(f"Columns: {', '.join(cols)}")
        logger.info(f"Total rows: {stg_count}\n")
        
        logger.info("=" * 60)
        logger.info("FACT MODEL (fct_weather_summary):")
        logger.info("=" * 60)
        cursor.execute("SELECT * FROM fct_weather_summary")
        result = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        logger.info(f"Columns: {', '.join(cols)}")
        for row in result:
            logger.info(row)
        
        logger.info("\nTransformations complete!")
        
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    execute_transformations()
