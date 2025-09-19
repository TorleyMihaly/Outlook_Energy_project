
from open_meteo_request import request
from SQLite_db_init import db_init
import pandas as pd


coordinates = {"London": [51.52, -0.12], "Bergen": [60.363, 5.294], "Norilsk": [69.350, 88.188]}

df = pd.DataFrame()

for city, coordinate in coordinates.items():
    hourly_df = request(coordinate=coordinate, city_name=city)
    df = pd.concat([df, hourly_df])


db_init(df)

