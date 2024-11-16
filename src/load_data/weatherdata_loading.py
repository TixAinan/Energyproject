from dotenv import load_dotenv
import sys
import os
import requests
import mysql.connector
import pandas as pd

sys.path.append("src")

# Loading .env
load_dotenv()

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT")
key = os.getenv("KEY")

limit = 100000
api_url = "https://dmigw.govcloud.dk/v2/climateData/collections/municipalityValue/items"
paramIds = ["mean_temp", "mean_wind_speed", "mean_wind_dir", "bright_sunshine", "mean_relative_hum"]
offset = 0
start_date = "2021-01-01T00:00:00"
end_date = "2021-01-31T00:00:00"  # remember correct number of days pr month.

#! ADD TYPESETTING

try:  # tests if connection to db is possible
    connection = mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_name, port=db_port)

    if connection.is_connected():
        print("Connection to MySQL was successful!")
    connection.close()

except mysql.connector.Error as err:
    print(f"Error: {err}")


def construct_api_url(url, key, offset, limit, start_date, end_date, paramId) -> str:
    return f"{url}?api-key={key}&offset={offset}&limit={limit}&datetime={start_date}Z/{end_date}Z&timeResolution=day&parameterId={paramId}"


def fetch_data_from_api(url: str):
    response = requests.get(url)
    if response.status_code == 200:
        response.raise_for_status()
        return response.json()
    else:
        print(f"Failed to fetch data from API: {response.status_code}")
        return None  # Return None on failure


def parse_transform_weather_data(features, paramId) -> pd.DataFrame:
    """Move data fra JSON to pd.df, renames and drops columns"""
    if not features:
        print("No data found in response.")
        return pd.DataFrame()

    df = pd.json_normalize(features)
    df = df.rename(columns={"properties.value": paramId})  # renames the value column or there will be multiple value columns when merged.

    columns_to_drop = [
        "id",
        "type",
        "geometry.coordinates",
        "geometry.type",
        "properties.calculatedAt",
        "properties.created",
        "properties.qcStatus",
        "properties.timeResolution",
        "properties.parameterId",
        "properties.to",
    ]  # the api returns a lot of columns that are not needed, which can be dropped.

    df = df.drop(columns=columns_to_drop, errors="ignore")
    return df


def load_weather_data(url, offset, start_date, end_date, limit, key, paramId) -> pd.DataFrame:
    """Main function to load weather data from the API."""
    api_url = construct_api_url(url, key, offset, limit, start_date, end_date, paramId)
    print("Requesting URL:", api_url)

    result = fetch_data_from_api(api_url)
    features = result.get("features")

    return parse_transform_weather_data(features, paramId)


merged_df = pd.DataFrame()

for paramId in paramIds:  # the dmi api can only get data from one weather feature at a time. loops and merges dataframes
    df = load_weather_data(api_url, offset, start_date, end_date, limit, key, paramId)

    if merged_df.empty:
        merged_df = df
    else:
        merged_df = merged_df.merge(df, on=["properties.from", "properties.municipalityId", "properties.municipalityName"])

merged_df.head()
merged_df.columns

# renames columns
merged_df = merged_df.rename(
    columns={
        "properties.from": "dateutc",
        "properties.municipalityId": "municipality_id",
        "properties.municipalityName": "municipality_name",
    }
)


# validating number of days
print(f"Number of unique days: {merged_df['dateutc'].nunique()}")
# validating number of municipalities
print(f"Number of municipalities: {merged_df['municipality_id'].nunique()}")
# add a proper check. Unittesting?
print(f"Number of rows: {len(merged_df)}")


# Create table query


# Insert into table query

# Create table in mysql function

# insert into table in mysql function

# run datapipeline function

# if main ...
