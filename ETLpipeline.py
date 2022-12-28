import time
import spotipy
import openpyxl
import requests
import configparser
import numpy as np
import pandas as pd
import psycopg2 as ps
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials

# load credentials(environment variables)
load_dotenv()

############ EXPORT DATA FROM API ############
# get top tracks from user spotify account
def get_top_tracks():
    # set scopes to fetch specified data
    scopes = ['user-top-read']
    # get credentials from spotify
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scopes))
    # get and store tracks
    top_tracks = sp.current_user_top_tracks()
    topTracks = []
    topArtists = []
    artist_IDs = []
    # retrieve top 10 tracks from user
    for idx, item in enumerate(top_tracks['items'][:10]):
        # print top ten tracks and artists
        #print(f"{idx} - {item['name']} by {[i['name'] for i in item['artists']]}")
        # add track name
        topTracks.append(item['name'])
        # add first artist, ignore featured artists
        topArtists.append(item['artists'][0]['name'])
        # add artist ID
        artist_IDs.append(item['artists'][0]['id'])
    return topTracks, topArtists, artist_IDs

def get_artist_top_tracks(artist_IDs):
        # create instance of spotipy with credentials
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials())
        # create array to store top tracks from each artist
        artist_top_tracks = []
        # loop through all artist ids
        for id in set(artist_IDs):
            temp_list = sp.artist_top_tracks(id, 'US')
            # add each track to the list
            artist_top_tracks.append([i['name'] for i in temp_list['tracks']])
            # wait 5 seconds for new request
            time.sleep(5)
        # return all tracks
        return artist_top_tracks

########### TRANSFORM DATA USING PANDAS ###########

def transform(topTracks, topArtists, artist_top_tracks):
    is_top_track = []
    # store the list as a numpy array
    init = np.array(artist_top_tracks)
    # make the 2d array to 1d
    flattened = init.flatten()
    # loop through the user's top tracks
    for track in topTracks:
        # check to see if the top tracks are the artists' top tracks
        if track in flattened:
            is_top_track.append('Yes')
        else:
            is_top_track.append('No')
    # create a dataframe using pandas 
    df = pd.DataFrame(list(zip(topTracks, topArtists, is_top_track)), columns=['user_top_tracks', 'artist', 'is_artist_top_track'])
    # return the dataframe
    return df

########## LOAD DATA TO POSTGRES DATABASE ##########

def check_if_track_exists(curr, track_name):
    # get user's top track
    query = ("""SELECT user_top_tracks FROM topspotifytracks WHERE user_top_tracks = %s;""")
    curr.execute(query, (track_name, ))
    # return true if track exists
    return curr.fetchone() is not None

def insert(curr, row):
    # insert all tracks into table
    query = ("""INSERT INTO topspotifytracks (user_top_tracks, artist, is_artist_top_track) VALUES(%s, %s, %s);""")
    insert_values = (row['user_top_tracks'], row['artist'], row['is_artist_top_track'])
    curr.execute(query, insert_values)

def load(df):
    config = configparser.ConfigParser()
    try:
        config.read('ETLpipeline.ini')
    except Exception as e:
        print('error trying to read configuration file', str(e))
    # read credentials from configuration file
    host_name = config['CONFIG']['host_name']
    dbname = config['CONFIG']['database']
    port = config['CONFIG']['port']
    username = config['CONFIG']['username']
    password = config['CONFIG']['password']

    # connect to the postgres database
    try: 
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
    except ps.OperationalError as e:
        print('error trying to connect to database', str(e))

    curr = conn.cursor()
    # check to see if track is already in the database, otherwise add it to the database
    for i, row in df.iterrows():
        if check_if_track_exists(curr, row['user_top_tracks']):
            continue
        else:
            insert(curr, row)

    # commit database changes
    conn.commit()
    # close connection
    conn.close()


if __name__ == "__main__":
    # extract data
    topTracks, topArtists, artist_IDs = get_top_tracks()
    time.sleep(3)
    artist_top_tracks = get_artist_top_tracks(artist_IDs)
    
    # transform data
    df = transform(topTracks, topArtists, artist_top_tracks)

    # create csv file or xlsx file to view the dataset
    df.to_csv('top_spotify_tracks.csv', sep='\t', encoding='utf-8')
    df.to_excel('top_spotify_tracks.xlsx', sheet_name='Top_Spotify_Tracks')

    # load data
    load(df)