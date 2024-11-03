import requests
import pandas as pd


def get_data_from_api(url: str) ->pd.DataFrame:
    """Gets data from api, converts data from json into pd.dataframe"""
    response = requests.get(url=url)
    result = response.json()
    records = result.get('records', [])
    df = pd.DataFrame(records)
    return df

def connect_to_mysql_db():
    try:
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            port = db_port
        )

        if connection.is_connected():
            print("Connection to MySQL was successful!")
        connection.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
