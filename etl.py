import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
"""
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    df.artist_latitude=df.artist_latitude.fillna(0).astype(int)
    df.artist_longitude=df.artist_longitude.fillna(0).astype(int)
    for i, row in df.iterrows():
        # insert song record
        song_data = [row.song_id,row.title,row.artist_id,int(row.year),int(row.duration)]
        cur.execute(song_table_insert, song_data)
    
        # insert artist record
        artist_data = [row.artist_id,row.artist_name,str(row.artist_location),int(row.artist_latitude),int(row.artist_longitude)]
        cur.execute(artist_table_insert, artist_data)

"""
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the logging information and converts the datetime column and stores data into different user and songplay tables.
    
    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
"""

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.where(df["page"]=="NextSong") 
    

    # convert timestamp column to datetime
    t =  pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ("start_time" , "hour" ,"day" ,"week" , "month" , "year" , "weekday")
    dict1={}
    j=0
    for i in column_labels:
        dict1[i]=time_data[j]
        j=j+1
    
    time_df = pd.DataFrame.from_dict(dict1) 
    time_df=time_df.astype(object).where(time_df.notnull(), None)
    time_df=time_df.dropna()
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    # load user table
    user_df =  df[['userId','firstName','lastName','gender','level']]
    user_df = user_df.where(pd.notnull(user_df), None)
    user_df=user_df.dropna()
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    df['ts'] =  pd.to_datetime(df['ts'], unit='ms')
    df.ts=df.ts.astype(object).where(df.ts.notnull(), None)
    df = df.where(pd.notnull(df), None)
    df=df.dropna()
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, int(row.length)))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
"""
    This procedure iterates all the files with matching extension for either log/song files whose filepath has been provided as an arugment.
    It gets the number of files found and based on func parameter passed respective function is called for further processesing to get the
    data and store into different tables.
    
    INPUTS: 
    * cur the cursor variable
    * conn variabele to connect to the date and commit the transactions
    * filepath the file path to the log file
    * func variable to process either song or log files as processed through all the files
"""


def process_data(cur, conn, filepath, func):
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

"""
    This main procedure connects to the postgres database and calls the respective function to process song and log files
"""

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()