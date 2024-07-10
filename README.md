Weather Data ETL Process
This project is designed to extract, transform, and load weather data from the Open-Meteo API into a CockroachDB data warehouse. It also includes functionality for sending alerts based on specific weather conditions.

Prerequisites
Python 3.7+
Required Python libraries:
datetime
pandas
weather (custom module)
datawarehouse (custom module)
json
alert (custom module)
logging
os

Script Overview
The script performs the following steps:

Set Coordinates and Date Range:
Sets the latitude and longitude for the location.
Determines the start date based on the maximum date of previously stored weather data.
Defines the end date for data extraction.

Logging Setup:
Configures logging to output to both console and a log file.

Data Extraction:
Extracts historical weather data from the Open-Meteo API.
If any data points are missing, attempts to fill in the gaps using forecast data.

Data Loading:
Loads the cleaned and complete weather data into CockroachDB.

Weather Alerts:
Checks for bad weather conditions.
Sends email alerts if any bad weather conditions are detected.

Example
INFO: Starting extraction, transformation, and loading from open-meteo.com...
INFO: ...weather data extracted from 2024-07-08 00:00:00 to 2024-07-08 23:00:00 
INFO: ...data have been loaded to CockroachDB
INFO: ...alerts are being sent
INFO: Closing extraction, transformation, and loading from open-meteo.com...

License
This project is licensed under the MIT License. See the LICENSE file for details.

Acknowledgements
Open-Meteo API for providing weather data.
