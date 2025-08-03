import os
import sys
from typing import Union

import mysql.connector
import pandas as pd
import requests
from dotenv import load_dotenv
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

sys.path.append("src")

# Loading .env
load_dotenv()

db_host: str | None = os.getenv("DB_HOST")
db_user: str | None = os.getenv("DB_USER")
db_password: str | None = os.getenv("DB_PASSWORD")
db_name: str | None = os.getenv("DB_NAME")
db_port: str | None = os.getenv("DB_PORT")
key: str | None = os.getenv("KEY")

limit: int = 10000
api_url: str = "https://dmigw.govcloud.dk/v2/climateData/collections/municipalityValue/items"
paramIds: list = [
    "mean_temp",
    "mean_wind_speed",
    "mean_wind_dir",
    "bright_sunshine",
    "mean_relative_hum",
]
offset: int = 0
start_date: str = "2021-01-01T00:00:00"
end_date: str = (
    "2021-01-31T00:00:00"  # remember correct number of days pr month.
)

# TODO Remove print statements to increase performance, when sure everything works


def run_data_pipeline(
    key: str | None,
    limit: int,
    api_url: str,
    paramIds: list,
    offset: int,
    start_date: str,
    end_date: str,
) -> None:
    """ETL Pipeline. load_merge_dataframes loops over the paramids to request data, create and merge dataframes,
    as its not possible to request more than one weather parameter at a time"""
    # check if connection to db is possible
    connection = create_db_connection(
        db_host, db_user, db_password, db_name, db_port
    )
    check_db_connection(connection)
    df = load_merge_dataframes(
        api_url, offset, start_date, end_date, limit, key, paramIds
    )
    df = rename_columns_in_merged(df)
    print(df.head())
    print(df.columns)
    print(df.dtypes)
    close_db_connection(connection)
    return None


# Create table query


# Insert into table query

# Create table in mysql function


# insert into table in mysql function
def create_db_connection(
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name,
    db_port=db_port,
) -> Union[PooledMySQLConnection, MySQLConnectionAbstract]:
    """Attempts to connect to mysql database. If succesfull return connects, else returns None"""

    connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        port=db_port,
    )
    return connection


def check_db_connection(
    connection: Union[
        PooledMySQLConnection,
        MySQLConnectionAbstract,
    ],
) -> None:
    if not connection.is_connected():
        raise mysql.connector.InterfaceError(
            "Not connected to database, Interface error"
        )


def close_db_connection(
    connection: Union[PooledMySQLConnection, MySQLConnectionAbstract],
) -> None:
    connection.close()


def construct_api_url(
    url: str,
    key: str | None,
    offset: int,
    limit: int,
    start_date: str,
    end_date: str,
    paramId: str,
) -> str:
    return f"{url}?api-key={key}&offset={offset}&limit={limit}&datetime={start_date}Z/{end_date}Z&timeResolution=day&parameterId={paramId}"


def get_data_from_api(url: str) -> pd.DataFrame:
    """Requests data from api_url. If succesfull transforms data from json to pd.DataFrame. If not returns an empty pd.DataFrame"""
    response = requests.get(url)
    if response.status_code == 200:
        response.raise_for_status()
        response = response.json()
        features = response.get("features")
        df = pd.json_normalize(features)
        return df
    else:
        print(f"Failed to fetch data from API: {response.status_code}")
        return pd.DataFrame()  # Return Empty DataFrame on failure


def rename_and_drop_columns(
    dataframe: pd.DataFrame, paramId: str
) -> pd.DataFrame:
    """Rename columns to remove the "properties." part. Drops unneeded columns"""
    if dataframe.empty:
        print("No data found in response.")
        return pd.DataFrame()

    df = dataframe.rename(
        columns={"properties.value": paramId}
    )  # renames the value column or there will be multiple value columns when merged.

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


def load_weather_data(
    url: str,
    offset: int,
    start_date: str,
    end_date: str,
    limit: int,
    key: str | None,
    paramId: str,
) -> pd.DataFrame:
    """Main function to load weather data from the API."""
    api_url = construct_api_url(
        url, key, offset, limit, start_date, end_date, paramId
    )
    print("Requesting URL:", api_url)

    data = get_data_from_api(api_url)
    data = rename_and_drop_columns(data, paramId)

    return data


def load_merge_dataframes(
    api_url: str,
    offset: int,
    start_date: str,
    end_date: str,
    limit: int,
    key: str | None,
    paramIds: list,
) -> pd.DataFrame:
    """It is only possible to request on parameter at a time from the API. This functions loads data for each parameter.
    Creates the dataframes for each and merges them.
    """
    merged_df = pd.DataFrame()
    # the dmi api can only get data from one weather feature at a time. loops and merges dataframes
    for paramId in paramIds:
        print(f"Currently requesting: {paramId}")
        df = load_weather_data(
            api_url, offset, start_date, end_date, limit, key, paramId
        )

        if merged_df.empty:
            merged_df = df
        else:
            merged_df = merged_df.merge(
                df,
                on=[
                    "properties.from",
                    "properties.municipalityId",
                    "properties.municipalityName",
                ],
            )
    return merged_df


def rename_columns_in_merged(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "properties.from": "dateutc",
            "properties.municipalityId": "municipality_id",
            "properties.municipalityName": "municipality_name",
        }
    )


"""
# validating number of days
print(f"Number of unique days: {merged_df['dateutc'].nunique()}")
# validating number of municipalities
print(f"Number of municipalities: {merged_df['municipality_id'].nunique()}")
# add a proper check. Unittesting?
print(f"Number of rows: {len(merged_df)}")"""


# Create table query


# Insert into table query

# Create table in mysql function

# insert into table in mysql function

# if main ...
# run datapipeline function
if __name__ == "__main__":
    run_data_pipeline(
        key=key,
        limit=limit,
        api_url=api_url,
        paramIds=paramIds,
        offset=offset,
        start_date=start_date,
        end_date=end_date,
    )
