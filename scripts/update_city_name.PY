# rename_city_pandas.py
import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "weather_star_user.db"



def main():
    try:
        loc_id = int(input("Location Id you want to change(must be an int): "))
    except:
        raise SystemExit("Input must be an integer")
    new_name = str(input("Name you want the location to change to: "))
    db_path = str(Path(DB_PATH).resolve())
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")  # ensure FK enforcement
    try:
        q = "SELECT * FROM dim_location WHERE location_id = ?;"
        before = pd.read_sql_query(q, conn, params=(loc_id,))
        print("Before change:")
        print(before)

        if before.empty:
            print(f"No dim_location row found for location_id = {loc_id}. Aborting.")
            return

        cur = conn.cursor()
        try:
            cur.execute("BEGIN;")
            cur.execute(
                "UPDATE dim_location SET station_code = ? WHERE location_id = ?;",
                (new_name, loc_id)
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        after = pd.read_sql_query(q, conn, params=(loc_id,))
        print("\nAfter change:")
        print(after)

        fk_check_sql = """
        SELECT
            COUNT(*) AS missing_fk_count
        FROM fact_weather f
        LEFT JOIN dim_location d ON f.location_id = d.location_id
        WHERE d.location_id IS NULL;
        """
        fk_result = pd.read_sql_query(fk_check_sql, conn)
        print("\nReferential integrity check (missing FK rows):")
        print(fk_result)

        #Proof
        facts = pd.read_sql_query("SELECT * FROM fact_weather WHERE location_id = ? LIMIT 10;", conn, params=(loc_id,))
        print(f"\nSample fact_weather rows for location_id={loc_id}:")
        print(facts)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
