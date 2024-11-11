from dotenv import load_dotenv
import sys
import os
import mysql.connector
import pandas as pd
import requests

sys.path.append("src") 

#Loading .env
load_dotenv()

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_port = int(os.getenv("DB_PORT"))

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

eds_url = "https://api.energidataservice.dk/dataset/ProductionMunicipalityHour?"
offset = 0
start_date = '2022-01-01T00:00'# Must be of from year-mm-ddThh:mm
end_date = '2022-01-14T00:00' # Must be of from year-mm-ddThh:mm

#api_url = 'https://api.energidataservice.dk/dataset/ProductionMunicipalityHour?offset=0&start=2022-01-01T00:00&end=2022-02-31T00:00&sort=HourUTC%20DESC'  #energidata url



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
    );
    """

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



def get_data_from_api(url: str) -> pd.DataFrame:
    """Gets data from api, converts data from json into pd.DataFrame"""
    response = requests.get(url)
    if response.status_code == 200:
        result = response.json()
        records = result.get('records', [])
        return pd.DataFrame(records)
    else:
        print(f"Failed to fetch data from API: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame on failure



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


api_url = construct_energy_api_url(eds_url, offset, start_date, end_date)
print(api_url)
df = get_data_from_api(api_url)
df = df.replace({float('nan'): None})
df['HourUTC'] = pd.to_datetime(df["HourUTC"])
df = df.drop(columns='HourDK', errors='ignore')

to_numeric_columns = [col for col in df.columns if not col.endswith('UTC')]
df[to_numeric_columns] = df[to_numeric_columns].apply(pd.to_numeric, errors='coerce')

df["HourUTC"] = df["HourUTC"].dt.date
mwh_columns = [col for col in df.columns if col.endswith('MWh')]
df = df.groupby(['HourUTC', 'MunicipalityNo'])[mwh_columns].agg('sum')
df = df.reset_index()

#df = df.resample('24h', on = 'HourUTC', group_keys = 'MunicipalityNo').agg({})
df.columns
df[df['MunicipalityNo']==360].head()


# Tranform from hourly to daily
df = df.resample('24h', on = 'HourUTC').sum()
print(df.head(5))

def run_data_pipeline(create_table_query,insert_into_query, url, offset, start_date, end_date):
    """Execute ETL pipeline.
    The data is originally hourly.
    Tranforms it to be daily.
    """
    api_url = construct_energy_api_url(url, offset, start_date, end_date)
    print(api_url)
    df = get_data_from_api(api_url)
    df = df.replace({float('nan'): None})
    print(df.head())
    # Tranform from hourly to daily
    df = df.resample('24H', on = 'HourUTC').sum()
    print(df.head(5))

    connection = create_db_connection()

    if connection is not None:
        create_table(connection,create_table_query)
        insert_energydata_into_table(connection, df, insert_into_query)


required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_PORT"]
for var in required_vars:
    if not os.getenv(var):
        print(f"Environment variable {var} is not set")

if __name__ == "__main__":
    run_data_pipeline(energy_table_query, insert_energydata_query, eds_url, offset, start_date, end_date)
    
