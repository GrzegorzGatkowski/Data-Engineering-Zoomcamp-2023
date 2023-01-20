import pandas as pd
import psycopg2
from sqlalchemy import create_engine

df = pd.read_csv('data/green_tripdata_2019-01.csv')


engine = create_engine('postgresql://root:root@localhost:5431/de_taxi')

engine.connect()

df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

df.to_sql(name='green_taxi', con = engine, index = False)

df_zones = pd.read_csv('data/taxi+_zone_lookup.csv')

df_zones.to_sql(name='zones', con = engine, index = False)





