import hashlib
import json
import os
import sqlite3
import time
from io import StringIO

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


def get_connection_db():
    conn = sqlite3.connect(os.environ.get("DB_NAME"))
    return conn


def get_regions():
    headers = {
        'x-rapidapi-key': os.environ.get("X-RAPIDAPI-KEY"),
        'x-rapidapi-host': os.environ.get("X-RAPIDAPI-HOST")
    }
    response = requests.get(os.environ.get("ALL_COUNTRIES_URL"), headers=headers)

    if response.status_code == 200:
        df = pd.read_json(response.text)
        regions = list(df['region'].unique())
        return regions
    return None


def insert_to_database(df):
    df.to_sql('country', con=conn, if_exists='append', chunksize=1000)


def get_country_by_region(regions, conn):
    response = requests.get("https://restcountries.eu/rest/v2/all")
    if response.status_code == 200:
        start_time = time.time()
        df = pd.read_json(StringIO(json.dumps(json.loads(response.text))), encoding='utf-8')
        df = df[df['region'].str.strip().astype(bool)]
        df['start_time'] = start_time
        df = df.drop_duplicates(subset=['region'])[['region', 'name', 'languages', 'start_time']]

        df['languages'] = df['languages'].apply(lambda x: hashlib.new("sha1", str(x[0]['name']).encode('utf-8')).hexdigest())
        df['time'] = round(time.time() - df['start_time'].iloc[0], 2)
        df = df.drop(columns=['start_time'])
        return df
    return None


def print_statistics(df):
    print('Total time elapsed:', df['time'].sum())
    print('Average time:', df['time'].sum() / len(df['time']))
    print('Max time:', df['time'].max())
    print('Min time:', df['time'].min())


def save_file_json(df):
    df.to_json('data.json', orient='records')


conn = get_connection_db()
regions = get_regions()
if regions is not None:
    df = get_country_by_region(regions, conn)
    if df is not None:
        insert_to_database(df)
        print_statistics(df)
        save_file_json(df)
    else:
        print("Ha ocurrido un error inesperado")
else:
    print("Ha ocurrido un error inesperado")
