import os
import sys

from typing import Union

import mysql.connector
import pandas as pd
import requests

from dotenv import load_dotenv
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract


sys.path.append("src")

# Loading .env
load_dotenv()

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT")

## Define the data we want from the api
eds_url: str = "https://api.energidataservice.dk/dataset/ProductionMunicipalityHour?"
offset: int = 0
start_date: str = "2022-01-02T00:00"  # Must be of from year-mm-ddThh:mm
end_date: str = "2022-01-04T00:00"  # Must be of from year-mm-ddThh:mm


def run_data_pipeline(
    create_table_query: str,
    insert_into_query: str,
    url: str,
    offset: int,
    start_date: str,
    end_date: str,
    batch_size: int,
) -> None:
    """Execute ETL pipeline. Connects to database. Gets data from api. Creates tables and inserts data"""

    connection = create_db_connection()
    check_db_connection(connection)
    api_url = construct_energy_api_url(url, offset, start_date, end_date)
    print(api_url)
    df = get_data_from_api(api_url)
    df = transform_data(df)
    create_table(connection, create_table_query)
    insert_energydata_into_table(connection, df, insert_into_query, batch_size)
    close_db_connection(connection)


energy_table_query = """
    CREATE TABLE IF NOT EXISTS energyproduction(
        `date` DATE,
        `municipality_no` VARCHAR(30),
        `solar_mwh` DOUBLE,
        `offshorewind_lt_100mw_mwh` DOUBLE,
        `offshorewind_ge_100mw_mwh` DOUBLE,
        `onshorewind_mwh` DOUBLE,
        `thermalpower_mwh` DOUBLE,
        PRIMARY KEY(`date`, `municipality_no`)
    );
    """

# TODO Figure out if this is needed
municipality_no_query = """
    CREATE TABLE IF NOT EXISTS municipalities(
        `municipality_no` INT,
        `municipality_name`VARCHAR(30),
    PRIMARY KEY(`municipality_no`)     
    );
    """

insert_energydata_query = """
    INSERT INTO energyproduction(
        date, 
        municipality_no, 
        solar_mwh,
        offshorewind_lt_100mw_mwh, 
        offshorewind_ge_100mw_mwh,
        onshorewind_mwh,
        thermalpower_mwh)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """


def get_data_from_api(url: str) -> pd.DataFrame:
    """Gets data from api, converts data from json into pd.DataFrame"""
    response = requests.get(url)
    if response.status_code == 200:
        result = response.json()
        records = result.get("records", [])
        return pd.DataFrame(records)
    else:
        print(f"Failed to fetch data from API: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame on failure


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


def check_db_connection(connection: Union[PooledMySQLConnection, MySQLConnectionAbstract]) -> None:
    if not connection.is_connected():
        raise mysql.connector.InterfaceError("Not connected to database, Interface error")


def close_db_connection(connection: Union[PooledMySQLConnection, MySQLConnectionAbstract]) -> None:
    connection.close()


def construct_energy_api_url(url: str, offset: int, start_date: str, end_date: str) -> str:
    return f"{url}offset={offset}&start={start_date}&end={end_date}&sort=HourUTC%20DESC"


def create_table(connection: Union[PooledMySQLConnection, MySQLConnectionAbstract], query: str) -> None:
    """Create a table if it does not exist in the Mysql Database"""

    CREATE_TABLE_SQL_QUERY = query

    try:
        cursor = connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        connection.commit()
        print("Table created successfully")
    except mysql.connector.Error as e:
        print(f"[CREATING TABLE ERROR]: '{e}'")
    finally:
        cursor.close()


def insert_energydata_into_table(
    connection: Union[PooledMySQLConnection, MySQLConnectionAbstract], df: pd.DataFrame, insert_query: str, batch_size: int
) -> None:
    cursor = connection.cursor()
    INSERT_DATA_QUERY = insert_query

    # Split data into batches.
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]
    try:
        for i in range(0, len(data_values_as_tuples), batch_size):
            batch = data_values_as_tuples[i : i + batch_size]
            cursor.executemany(INSERT_DATA_QUERY, batch)
            connection.commit()
        print("Data inserted successfully")
    except mysql.connector.Error as e:
        print(f"[INSERTING DATA ERROR]: '{e}'")
    finally:
        cursor.close()


def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    # TODO Refactor into smaller functions
    "Replaces nan with None, drops unneeded columns and transforms from hourly to daily"
    if data is not None:
        data = data.replace({float("nan"): None})
        # Tranform from hourly to daily
        data["HourUTC"] = pd.to_datetime(data["HourUTC"])
        data["HourUTC"] = data["HourUTC"].dt.date
        data = data.rename(columns={"HourUTC": "Date"})
        data = data.drop(columns="HourDK", errors="ignore")

        # The api includes the last hour of the day before the start date. The following removes this.
        data = data.sort_values(by="Date")
        first_date = data.iloc[0]["Date"]
        data = data[data["Date"] != first_date]

        # transforms datatypes to numeric as its needed for grouping to daily
        to_numeric_columns = [col for col in data.columns if not col.endswith("Date")]
        data[to_numeric_columns] = data[to_numeric_columns].apply(pd.to_numeric, errors="coerce")
        mwh_columns = [col for col in data.columns if col.endswith("MWh")]

        # Tranform from hourly to daily
        data = data.groupby(["Date", "MunicipalityNo"])[mwh_columns].agg("sum")
        data = data.reset_index()  # needed to get back hour and municipality columns

        return data
    else:
        print("Dataframe is empty")


if __name__ == "__main__":
    run_data_pipeline(
        energy_table_query,
        insert_energydata_query,
        eds_url,
        offset,
        start_date,
        end_date,
        batch_size=10000,
    )
