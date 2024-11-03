from dotenv import load_dotenv
import sys
import os
import mysql.connector
import pandas as pd

sys.path.append("src") 
import dataloading_functions as dl

#Loading .env
load_dotenv()

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT")

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

## Define the data we want from the api and send a get request

api_url = 'https://api.energidataservice.dk/dataset/ProductionMunicipalityHour?offset=0&start=2022-01-01T00:00&end=2023-01-31T00:00&sort=HourUTC%20DESC'  #energidata url

df = dl.get_data_from_api(api_url)

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

energy_table_query = """
    CREATE TABLE IF NOT EXISTS energyproduction (
        `hourutc` DATETIME,
        `hourdk` DATETIME,
        `municipality_no` VARCHAR(30),
        `solar_mwh` DOUBLE,
        `offshorewind_lt_100mw_mwh` DOUBLE,
        `offshorewind_ge_100mw_mwh` DOUBLE,
        `onshorewind_mwh` DOUBLE,
        `thermalpower_mwh` DOUBLE,
        PRIMARY KEY(`hourutc`, `municipality_no`)
    );
    """
municipality_no_query= """
    CREATE TABLE IF NOT EXISTS municipalities(
    `municipality_no` INT,
    `municipality_name`VARCHAR(30),
    PRIMARY KEY(`municipality_no`)     
    );"""

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

insert_energydata_query = """
    INSERT INTO energyproduction(hourutc, 
    hourdk, 
    municipality_no, 
    solar_mwh,
    offshorewind_lt_100mw_mwh, 
    offshorewind_ge_100mw_mwh,
    onshorewind_mwh,
    thermalpower_mwh)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

insert_municipality_query = """
    INSERT INTO municipalities(municipality_no, municipality_name)"""

"""def insert_into_table(connection, df, insert_query):
    """"""
    cursor = connection.cursor()
    INSERT_DATA_QUERY = insert_query

    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]
    
    try:
        # Execute the query
        cursor.executemany(INSERT_DATA_QUERY, data_values_as_tuples)
        connection.commit()
        print("Data inserted or updated successfully")
    except mysql.connector.Error as e:
        print(f"[INSERTING DATA ERROR]: '{e}'")
    finally:
        cursor.close()"""

def insert_into_table(connection, df, insert_query, batch_size=10000):
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

def run_data_pipeline(create_table_query,insert_into_query):
    """Execute ETL pipeline
    """
    df = dl.get_data_from_api(api_url)
    df = df.replace({float('nan'): None})

    connection = create_db_connection()

    if connection is not None:
        create_table(connection,create_table_query)
        insert_into_table(connection, df, insert_into_query)



if __name__ == "__main__":
    run_data_pipeline(energy_table_query, insert_energydata_query)
    
