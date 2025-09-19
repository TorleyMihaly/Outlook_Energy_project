from pathlib import Path
import sqlite3
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "weather_star_user.db"

def observed_ts_cutoff(hours):
    tz = ZoneInfo("Europe/London")
    now = datetime.now(tz)
    cutoff = now + timedelta(hours=hours)
    print(cutoff.strftime("%Y-%m-%d %H:%M:%S"))
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")

def main():
    db_path = os.path.abspath(DB_PATH)
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {db_path}")
    input_time = input("Enter number of hours lookback for average temp: ")
    try:
        if int(input_time) > 168:
            raise SystemExit("Input must be less than 168 hours")
    except:
        raise SystemExit("Input must be an integer")
    cutoff = observed_ts_cutoff(int(input_time))
    conn = sqlite3.connect(db_path)
    sql = """
        SELECT
            location_id,
            AVG(temperature_2m) AS avg_temp,
            COUNT(*) AS n_obs
        FROM fact_weather
        WHERE location_id IN (1,2,3)
            AND observed_ts <= ?
        GROUP BY location_id
        ORDER BY location_id;
        """
    df = pd.read_sql_query(sql, conn, params=(cutoff,))
    print(df.head(20))
    conn.close()

if __name__ == "__main__":
    main()
