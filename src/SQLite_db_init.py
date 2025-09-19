import sqlite3
import pandas as pd
import os
from IPython.display import display
from pathlib import Path

repo_root = Path(__file__).resolve().parent.parent
data_dir = repo_root / "data"
data_dir.mkdir(parents=True, exist_ok=True)

db_path = data_dir / "weather_star_user.db"
db_path = str(db_path)         


def safe_connect(db_path, timeout=30):
    db_path = os.path.abspath(db_path)
    parent = os.path.dirname(db_path)

    if parent and not os.path.exists(parent):
        try:
            os.makedirs(parent, exist_ok=True)
        except Exception as e:
            raise OSError(f"Could not create parent directory {parent!r}: {e}")

    try:
        test_path = os.path.join(parent or ".", ".write_test_tmp")
        with open(test_path, "w") as f:
            f.write("ok")
        os.remove(test_path)
    except Exception as e:
        raise PermissionError(f"No write permission in directory {parent!r}: {e}")

    if os.path.isdir(db_path):
        raise IsADirectoryError(f"The path {db_path!r} is a directory, not a file.")
    
    try:
        conn = sqlite3.connect(db_path, timeout=timeout)
        return conn
    except Exception as e:
        raise sqlite3.OperationalError(f"sqlite3.connect failed for {db_path!r}: {e}")

def db_init(df):
    # column mapping
    date_col = "date"
    city_col = "city"
    latitude = "latitude"
    longitude = "longitude"
    measures = ["temperature_2m", "wind_speed_10m", "wind_chill"]

    # time dimension
    df[date_col] = pd.to_datetime(df[date_col])
    df["ts_hour"] = df[date_col].dt.floor("h")

    time_dim = (
        df[["ts_hour"]].drop_duplicates().sort_values("ts_hour").reset_index(drop=True)
        .rename(columns={"ts_hour":"ts"})
    )
    time_dim["time_id"] = (time_dim.index + 1).astype(int)
    time_dim["ts_iso"] = time_dim["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    time_dim["year"] = time_dim["ts"].dt.year.astype(int)
    time_dim["month"] = time_dim["ts"].dt.month.astype(int)
    time_dim["day"] = time_dim["ts"].dt.day.astype(int)
    time_dim["hour"] = time_dim["ts"].dt.hour.astype(int)
    time_dim["weekday"] = time_dim["ts"].dt.weekday.astype(int)
    time_dim["is_weekend"] = time_dim["weekday"].isin([5,6]).astype(int)
    time_dim["iso_week"] = time_dim["ts"].dt.isocalendar().week.astype(int)
    time_dim["quarter"] = time_dim["ts"].dt.quarter.astype(int)
    time_dim = time_dim[["time_id","ts_iso","year","month","day","hour","weekday","is_weekend","iso_week","quarter"]]

    # location dimension
    location_dim = (
        df[[city_col, latitude, longitude]]
        .drop_duplicates()
        .rename(columns={city_col: "station_code"})
        .reset_index(drop=True)
    )
    location_dim["location_id"] = (location_dim.index + 1).astype(int)
    location_dim = location_dim[["location_id","station_code","latitude","longitude"]]

    # Fact table
    fact = df.copy()
    fact["ts_iso"] = fact["ts_hour"].dt.strftime("%Y-%m-%d %H:%M:%S")
    fact = fact.merge(time_dim[["time_id","ts_iso"]], on="ts_iso", how="left")
    fact = fact.merge(location_dim[["location_id","station_code"]], left_on=city_col, right_on="station_code", how="left")

    fact_table = fact[["time_id","location_id"] + measures].copy()
    fact_table["observed_ts"] = fact[date_col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Create DB
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = safe_connect(db_path, timeout=30)
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    create_sql = """
    CREATE TABLE IF NOT EXISTS dim_time (
        time_id INTEGER PRIMARY KEY,
        ts_iso TEXT UNIQUE,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        hour INTEGER,
        weekday INTEGER,
        is_weekend INTEGER,
        iso_week INTEGER,
        quarter INTEGER
    );
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id INTEGER PRIMARY KEY,
        station_code TEXT UNIQUE,
        latitude REAL,
        longitude REAL
    );
    CREATE TABLE IF NOT EXISTS fact_weather (
        fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
        time_id INTEGER,
        location_id INTEGER,
        temperature_2m REAL,
        wind_speed_10m REAL,
        wind_chill REAL,
        observed_ts TEXT,
        FOREIGN KEY(time_id) REFERENCES dim_time(time_id),
        FOREIGN KEY(location_id) REFERENCES dim_location(location_id)
    );
    """
    cur.executescript(create_sql)

    time_tuples = [tuple(x) for x in time_dim.to_numpy()]
    loc_tuples = [tuple(x) for x in location_dim[["location_id","station_code","latitude","longitude"]].to_numpy()]
    fact_tuples = [tuple(x) for x in fact_table[["time_id","location_id","temperature_2m","wind_speed_10m","wind_chill","observed_ts"]].to_numpy()]

    cur.executemany(
        "INSERT OR IGNORE INTO dim_time (time_id, ts_iso, year, month, day, hour, weekday, is_weekend, iso_week, quarter) VALUES (?,?,?,?,?,?,?,?,?,?);",
        time_tuples
    )
    cur.executemany(
        "INSERT OR IGNORE INTO dim_location (location_id, station_code, latitude, longitude) VALUES (?,?,?,?);",
        loc_tuples
    )
    cur.executemany(
        "INSERT INTO fact_weather (time_id, location_id, temperature_2m, wind_speed_10m, wind_chill, observed_ts) VALUES (?,?,?,?,?,?);",
        fact_tuples
    )
    conn.commit()

    # Sample data
    df_time = pd.read_sql_query("SELECT * FROM dim_time ORDER BY time_id LIMIT 10;", conn)
    df_loc = pd.read_sql_query("SELECT * FROM dim_location ORDER BY location_id LIMIT 10;", conn)
    df_fact = pd.read_sql_query("SELECT * FROM fact_weather ORDER BY fact_id LIMIT 10;", conn)

    display("dim_time (head)", df_time)
    display("dim_location (head)", df_loc)
    display("fact_weather (head)", df_fact)

    conn.close()

    print(f"SQLite database created at: {db_path}")
