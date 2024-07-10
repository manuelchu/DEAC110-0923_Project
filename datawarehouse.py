from datetime import datetime
from typing import Any

import psycopg2
import pandas as pd


def connect_to_cockroachdb():
    conn = psycopg2.connect(
        dbname='defaultdb',
        user='user',
        password='***',
        host='***.cockroachlabs.cloud',
        port='26257',  # Default port for CockroachDB
        sslmode='require'  # Ensure SSL is used for connections
    )
    return conn


def store_weather_data_in_cockroachdb(data, logger):
    # Insert data into CockroachDB
    # print(data)
    for i in range(len(data)):
        row = data.iloc[i]
        if find_data(row) is False:
            # print(datetime.today())
            # Only insert data with date/time lower than actual date/time
            if row["date"] < datetime.today():
                conn = connect_to_cockroachdb()
                cur = conn.cursor()
                cur.execute('''
                    INSERT INTO public.weather
                    ("date", temperature_celsius, temperature_fahrenheit, relative_humidity, precipitation, rain, snowfall, snow_depth, weather_code, wind_speed)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                ''', (
                    row['date'],
                    round(float(row["temperature_celsius"]), 1),
                    round(float(row["temperature_fahrenheit"]), 1),
                    round(float(row["relative_humidity"]), 0),
                    round(float(row["precipitation"]), 2),
                    round(float(row["rain"]), 2),
                    round(float(row["snowfall"]), 2),
                    round(float(row["snow_depth"]), 2),
                    int(row["weather_code"]),
                    round(float(row["wind_speed"]), 1)
                ))
                conn.commit()
                cur.close()
                conn.close()
                logger.info(f".....(OK) row {row['date']} is loaded in data warehouse")
            else:
                logger.info(f".....(STOP) row {row['date']} is greater than actual date/time")
                break
        else:
            logger.info(f".....(WARNING) row {row['date']} is already exists in data warehouse")
    return True


def get_max_date_from_weather() -> Any | None:
    global cur, conn
    try:
        # Connect to CockroachDB
        conn = connect_to_cockroachdb()

        # Define the query
        query = "SELECT MAX(\"date\") AS max_date FROM public.weather;"

        # Create a cursor and execute the query
        cur = conn.cursor()
        cur.execute(query)

        # Fetch the result (single row with the maximum date)
        max_date_row = cur.fetchone()

        # Extract and return the maximum date if found
        if max_date_row:
            return max_date_row[0]
        else:
            return None
    except psycopg2.Error as error:
        raise error  # Re-raise the exception for handling at the caller level
    finally:
        # Close cursor and connection (if open)
        if cur:
            cur.close()
        if conn:
            conn.close()


def find_data(row) -> Any | None:
    global cur, conn
    try:
        # Connect to CockroachDB
        conn = connect_to_cockroachdb()

        # Define the query
        query = f"SELECT date FROM public.weather WHERE date='{row["date"]}'"

        # Create a cursor and execute the query
        cur = conn.cursor()
        cur.execute(query)

        # Fetch the result (single row with the maximum date)
        result = cur.fetchone()

        # Extract and return the maximum date if found
        if result:
            return True
        else:
            return False
    except psycopg2.Error as error:
        raise error  # Re-raise the exception for handling at the caller level
    finally:
        # Close cursor and connection (if open)
        if cur:
            cur.close()
        if conn:
            conn.close()