import sqlalchemy
import sqlite3
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime as dt
from pprint import pprint

DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
USER_ID = '314fk6fg3zwfxzzvsfpzktv6qgle'
TOKEN = 'BQAVq1Mb7aP51pt7knvDooEuf8qvAsF74VV-u2hoERoRQm5ZFkDaCokWRVD9SKtVtLlDx6PZhucxLSeZgEJ_T7yYis5mNHbT9qOdgcKdZaiRS-4gG5Qs9a9b-KQIVA0_-WkJrwpbYRHRtP-lxg2SycPptYKEgkDi9e4lXOZZ'

def validation(df):
    if df.empty:
        print('No songs were listened to the last 24 hours')
        return False
    
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Primary Key Check is violated')
    
    if df.isnull().values.any():
        raise Exception('Null value founded')

if __name__ == '__main__':
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {TOKEN}'
   }

    today = dt.datetime.now()
    yesterday = today - dt.timedelta(days = 1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(
            f'https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}',
            headers=headers)
    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    dates_list = []

    # pprint(data['items'][0]['track']['album']['artists'][0]['name'])
    for record in data['items']:
        song = record['track']['name']
        artist = record['track']['album']['artists'][0]['name']
        song_names.append(song)
        artist_names.append(artist)
        played_at_list.append(record['played_at'])
        dates_list.append(record['played_at'][0:10])

    song_dict = {
        'song_name': song_names,
        'artist_name': artist_names,
        'played_at': played_at_list,
        'played_date': dates_list
    }

    song_df = pd.DataFrame(song_dict, columns = ['song_name', 'artist_name', 'played_at', 'played_date'])

    if validation(song_df):
        print('Data is valid, proceed to Load Stage')
    print(song_df)

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        played_date VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")
    
