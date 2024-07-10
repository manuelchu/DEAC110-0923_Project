from datetime import timedelta
from datetime import datetime
import pandas as pd
import weather
import datawarehouse
import json
import alert
import logging, handler
import os

if __name__ == '__main__':
    latitude = 49.24
    longitude = -123.05
    max_date = datawarehouse.get_max_date_from_weather()
    if max_date is None:
        max_date = pd.to_datetime("2015-01-01")
    else:
        max_date = max_date + timedelta(hours=1)
        max_date = max_date.replace(hour=00, minute=00, second=00)

    start_date = pd.to_datetime(max_date)
    # start_date = pd.to_datetime("2024-07-08")  # For testing purposes
    end_date = start_date # Daily
    # end_date = start_date  + pd.offsets.MonthEnd(0) # Monthly
    end_date = end_date.replace(hour=23, minute=00, second=00)
    # print(f"Start date: {start_date}")
    # print(f"End date: {end_date}")

    log_file_path = f"{os.path.dirname(os.path.abspath(__file__))}\\logs\\{datetime.now().strftime("%Y%m%d%H%M%S")}_{start_date.strftime("%Y%m%d")}.log"
    logging.basicConfig(level=logging.INFO, handlers=[handler.DualOutputHandler(log_file_path)])
    logger = logging.getLogger()

    logger.info(f"Starting extraction, transformation, and loading from open-meteo.com... ")
    # hdf: Weather Historical Dataframe
    hdf = weather.get_weather_data(latitude, longitude, start_date, end_date)
    if len(hdf) > 0:
        # print(hdf.shape)
        # print(hdf)
        logger.info(f"...weather data extracted from {start_date.strftime("%Y-%m-%d %H:%M:%S")} to {end_date.strftime("%Y-%m-%d %H:%M:%S")} ")
        # print(hdf[hdf.isna().any(axis=1)])
        if len(hdf[hdf.isna().any(axis=1)]) > 0:
            # fdf: Weather Forecast Dataframe
            fdf = weather.get_weather_data(latitude, longitude, start_date, end_date, "forecast")
            # print(fdf)
            for i in range(len(fdf)):
                row = fdf.iloc[i]
                # print(type(row))
                hdf.loc[hdf['date'] == row['date'], "temperature_celsius"] = row["temperature_celsius"]
                hdf.loc[hdf['date'] == row['date'], "temperature_fahrenheit"] = row["temperature_fahrenheit"]
                hdf.loc[hdf['date'] == row['date'], "relative_humidity"] = row["relative_humidity"]
                hdf.loc[hdf['date'] == row['date'], "precipitation"] = row["precipitation"]
                hdf.loc[hdf['date'] == row['date'], "rain"] = row["rain"]
                hdf.loc[hdf['date'] == row['date'], "snowfall"] = row["snowfall"]
                hdf.loc[hdf['date'] == row['date'], "snow_depth"] = row["snow_depth"]
                hdf.loc[hdf['date'] == row['date'], "weather_code"] = row["weather_code"]
                hdf.loc[hdf['date'] == row['date'], "wind_speed"] = row["wind_speed"]
                # print(hdf.loc[hdf['date'] == row['date']])
    else:
        logger.info(f"...weather data couldn't extracted")
        exit(1)

    if datawarehouse.store_weather_data_in_cockroachdb(hdf, logger) is True:
        logger.info(f"...data have been loaded to CockroachDB")
    else:
        logger.info(f"...data haven't been loaded to CockroachDB")
        exit(1)

    bad_weather = weather.check_bad_weather(hdf)
    if len(bad_weather) > 0:
        logger.info(f"...alerts are being sent")
        alert.send_alert_email(bad_weather, logger)
    else:
        logger.info(f"...alerts for bad weather not found")

    logger.info(f"Closing extraction, transformation, and loading from open-meteo.com...")
