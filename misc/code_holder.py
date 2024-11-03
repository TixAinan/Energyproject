create_table(create_db_connection(), energy_table_query)
create_table(create_db_connection(), municipality_no_query)

insert_into_table(create_db_connection(),df, insert_energydata_query)

df.describe()

df = df.sort_values(by=['MunicipalityNo','HourUTC'])
df.reset_index(drop=True, inplace=True)

df.head()

df.MunicipalityNo.unique()

df = dl.get_data_from_api(api_url)
df = df.replace({float('nan'): None})
