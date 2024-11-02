import requests
import pandas as pd

response = requests.get(
    url='https://api.energidataservice.dk/dataset/ProductionMunicipalityHour?offset=0&start=2024-10-01T00:00&end=2024-11-01T00:00&sort=HourUTC%20DESC&limit=500000')

result = response.json()

records = result.get('records', [])

df = pd.DataFrame(records)

df.describe()

df = df.sort_values(by=['MunicipalityNo','HourUTC'])

df.head(50)