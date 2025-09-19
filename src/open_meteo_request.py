import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry
from decimal import *

cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

def request(coordinate: list[float], city_name: str):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": coordinate[0],
        "longitude": coordinate[1],
        "hourly": ["temperature_2m", "wind_speed_10m"],
        "timezone": "GMT",
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(1).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    def wind_chill_calc(temperature: float, speed: float):
        return 13.12 + 0.6215*temperature - 11.37*pow(speed, 0.16) + 0.3965*temperature*pow(speed, 0.16)

    hourly_data["city"] = city_name
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["latitude"] = round(response.Latitude(), 2)
    hourly_data["longitude"] = round(response.Longitude(), 2)
    hourly_data["wind_chill"] = wind_chill_calc(temperature=hourly_data["temperature_2m"], speed=hourly_data["wind_speed_10m"])

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    return(hourly_dataframe)
