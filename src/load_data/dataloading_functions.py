import requests
import pandas as pd
import mysql.connector

def get_data_from_api(url: str) ->pd.DataFrame:
    """Gets data from api, converts data from json into pd.dataframe"""
    response = requests.get(url=construct_energy_api_url)
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

def create_db_connection(db_host=db_host, db_user=db_user, db_password=db_password, db_name=db_name, db_port=db_port):
    """Attempts to connect to mysql database. If succesfull return connects, else returns None"""
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
            return connection

    except mysql.connector.Error as err: 
        print(f"Error: {err}")
        
    return None

def construct_energy_api_url(url, offset,  start_date, end_date)-> str:
    return f"{url}offset={offset}&start={start_date}&end={end_date}&sort=HourUTC%20DESC"

def create_table(connection, query):
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
        cursor.close()  # Close the cursor to free resources


def insert_energydata_into_table(connection, df, insert_query, batch_size=10000):
    cursor = connection.cursor()
    INSERT_DATA_QUERY = insert_query

    # Split data into batches
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]
    try:
        for i in range(0, len(data_values_as_tuples), batch_size):
            batch = data_values_as_tuples[i:i + batch_size]
            cursor.executemany(INSERT_DATA_QUERY, batch)
            connection.commit()
        print("Data inserted successfully")
    except mysql.connector.Error as e:
        print(f"[INSERTING DATA ERROR]: '{e}'")
    finally:
        cursor.close()

def run_data_pipeline(create_table_query,insert_into_query,api_url):
    """Execute ETL pipeline
    """
    df = get_data_from_api(construct_energy_api_url())
    df = df.replace({float('nan'): None})

    connection = create_db_connection()

    if connection is not None:
        create_table(connection,create_table_query)
        insert_energydata_into_table(connection, df, insert_into_query)

