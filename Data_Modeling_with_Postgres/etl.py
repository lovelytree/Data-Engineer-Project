import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Arguments: 
        cur : cursor to the connection of database
        filepath : directory of the song file
        
    - Read song file using pandas dataframe
    - Extract artist data and insert it into artists table
    - Extract song data and insert it into songs table
    """
    
    # open and read song file
    df = pd.read_json(filepath, lines="True")

    # extract artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_data = artist_data.values[0] 
    
    # insert artist record into database
    cur.execute(artist_table_insert, artist_data)
    
    # extract song record
    song_data = df[['song_id','title','artist_id','year','duration']]
    song_data = song_data.values[0]
    
    # insert song record into database
    cur.execute(song_table_insert, song_data)
    

def process_log_file(cur, filepath):
    """
    - Arguments: 
        cur : cursor to the connection of database
        filepath : directory of the log file
        
    - Read log file using pandas dataframe
    - Filter the log data by page == "NextSong"
    - For 'ts' column, convert timestamp to datetime
    - Extract time data, insert it into time table 
    - Extract user data, insert it into users table
    - Extract songplay data, insert it into songplays table
    """
        
    # open log file
    df = pd.read_json(filepath, lines="True")

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # Extract time data records
    time_data = {}
    time_data['ts'] = t
    time_data['hour'] = t.dt.hour
    time_data['day'] = t.dt.day
    time_data['week'] = t.dt.weekofyear
    time_data['month'] = t.dt.month
    time_data['year'] = t.dt.year
    time_data['weekday'] = t.dt.dayofweek
    
    time_df = pd.DataFrame(data=time_data)

    # insert time data records into database
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # extract user records
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records into database
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts,unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Get all the json files from directory 'filepath', and put their abosulte path to a list
    - Get the total number of files found
    - For each file, call the "func" to process
    """
        
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connect to the database sparkifydb and get cursor to it
    
    - Read and process song files under 'data/song_data'

    - Read and process log files under 'data/log_data'  
    
    - Finally, Close the connection 
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()