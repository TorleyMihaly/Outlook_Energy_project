from pathlib import Path
import sqlite3
import pandas as pd
import os

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "weather_star_user.db"

def main():
    db_path = os.path.abspath(DB_PATH)
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT MAX(wind_speed_10m), observed_ts, location_id
            FROM fact_weather 
            WHERE location_id = 1
        UNION
        SELECT MAX(wind_speed_10m), observed_ts, location_id
            FROM fact_weather 
            WHERE location_id = 2
        UNION
        SELECT MAX(wind_speed_10m), observed_ts, location_id
            FROM fact_weather 
            WHERE location_id = 3;
        """, conn)
    print(df.head(20))
    conn.close()

if __name__ == "__main__":
    main()
