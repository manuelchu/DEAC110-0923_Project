from datetime import timedelta

import numpy
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from pytz import timezone


def get_weather_data(latitude, longitude, start_date, end_date, mode="archive"):
    # Set up the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = f"https://archive-api.open-meteo.com/v1/{mode}" if mode == "archive" else f"https://api.open-meteo.com/v1/{mode}"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.date(),
        "end_date": end_date.date(),
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "snowfall", "snow_depth",
                   "weather_code", "wind_speed_10m"],
        "timezone": "auto"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    # print(f"Elevation {response.Elevation()} m asl")
    # print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    # print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    # print(hourly.Time())

    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
    hourly_rain = hourly.Variables(3).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(4).ValuesAsNumpy()
    hourly_snow_depth = hourly.Variables(5).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(6).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(7).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time() + response.UtcOffsetSeconds(), unit="s", utc=False),
        end=pd.to_datetime(hourly.TimeEnd() + response.UtcOffsetSeconds(), unit="s", utc=False),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "temperature_celsius": hourly_temperature_2m,
        "temperature_fahrenheit": (hourly_temperature_2m * 9 / 5) + 32,
        "relative_humidity": hourly_relative_humidity_2m,
        "precipitation": hourly_precipitation,
        "rain": hourly_rain,
        "snowfall": hourly_snowfall,
        "snow_depth": hourly_snow_depth,
        "weather_code": hourly_weather_code,
        "wind_speed": hourly_wind_speed_10m
    }

    # print(type(hourly_data))
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    pd.set_option('display.max_columns', None)
    return hourly_dataframe


def identify_data_with_nan_values(data):
    df = pd.DataFrame(columns=data.columns)
    df = data.isna().any(axis=1)
    # for i in range(len(df)):
    #    if i.iloc[1] is True:
    # Print rows with NaN values (optional)
    # print(data[df])
    return df


def check_bad_weather(data):
    dt = None
    bad_weather = []
    for i in range(len(data)):
        row = data.iloc[i]

        dt = pd.to_datetime(row['date'])

        # Temperature:
        if row["temperature_celsius"] < 0:
            bad_weather.append([dt, "Temperature Below freezing (0°C):",
                                f"{round(float(row["temperature_celsius"]), 2)}°C",
                                "Potential for frost, ice, or hazardous driving conditions."])
        elif row["temperature_celsius"] > 35:
            bad_weather.append([dt, "High temperatures above 35°C",
                                f"{round(float(row["temperature_celsius"]), 2)}°C",
                                "Cause heat-related illnesses and discomfort."])

        # Rain:
        if row["rain"] > 10:
            bad_weather.append([dt, "Heavy rain (above 10mm/hour)",
                                f"{round(float(row["rain"]), 2)} mm/hour",
                                "Cause flooding, reduced visibility, and hazardous driving conditions."])

        # Snowfall:
        if row["snowfall"] > 5:
            bad_weather.append([dt, "Significant snowfall (above 5cm/hour)",
                                f"{round(float(row["snowfall"]), 2)} cm/hour",
                                "Travel disruptions, potential road closures."])

        # Snow Depth:
        if row["snow_depth"] > 0.5:
            bad_weather.append([dt, "Deep snow (above 0.5 m)",
                                f"{round(float(row["snow_depth"]), 2)} m",
                                "Movement hindrance, structural damage, and risk of avalanches in mountainous areas."])

        # Wind Speed:
        if row["wind_speed"] > 50:
            bad_weather.append([dt, "High winds (above 50 km/h)",
                                f"{round(float(row["wind_speed"]), 2)} km/h",
                                "Property damage, power outages, hazardous driving conditions."])

    return bad_weather
