import os
import sys
import requests
import logging
from dotenv import load_dotenv
from typing import Optional

import psycopg

import polars as pl


sys.path.append("src")

load_dotenv()

logger = logging.getLogger(__name__)

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT")

## Define the data we want from the api
eds_url: str = (
    "https://api.energidataservice.dk/dataset/ProductionMunicipalityHour?"
)
offset: int = 0
start_date: str = "2022-01-02T00:00"  # Must be of from year-mm-ddThh:mm
end_date: str = "2022-01-04T00:00"  # Must be of from year-mm-ddThh:mm


def construct_energy_api_url(
    url: str, offset: int, start_date: str, end_date: str
) -> str:
    """Construct url to access the energydata."""
    return f"{url}offset={offset}&start={start_date}&end={end_date}&sort=HourUTC%20DESC"


def get_data(url: str) -> Optional[pl.DataFrame]:
    """Get data from EDS API."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        result = response.json()
        records = result.get("records", [])
        return pl.DataFrame(records)
    except (requests.RequestException, ValueError):
        logger.exception("Something went wrong")
        return None


if __name__ == "__main__":
    url = construct_energy_api_url(eds_url, offset, start_date, end_date)
    df = get_data(url=url)
    df.describe()
