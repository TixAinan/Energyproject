import requests
import pandas

response = requests.get(
    url='https://api.energidataservice.dk/dataset/ProductionMunicipalityHour?offset=0&start=2024-10-01T00:00&end=2024-11-01T00:00&sort=HourUTC%20DESC&limit=20')

result = response.json()

for k, v in result.items():
    print(k, v)

records = result.get('records', [])

print('records:')
for record in records:
    print(' ', record)

records.keys()
