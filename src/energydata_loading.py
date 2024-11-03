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

api_url = 'https://api.energidataservice.dk/dataset/ProductionMunicipalityHour?offset=0&start=2024-10-01T00:00&end=2024-11-01T00:00&sort=HourUTC%20DESC&limit=50'  #energidata url

df = dl.get_data_from_api(api_url)

df.describe()

df = df.sort_values(by=['MunicipalityNo','HourUTC'])

df.info()
df.head(50)

df.MunicipalityNo.unique()

mun_no = pd.read_csv(r"MunicipalityNo.csv")

mun_no.info()