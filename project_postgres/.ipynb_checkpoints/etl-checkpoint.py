"""
    Author:
        Udacity, RichaS
    Date Created:
        05/25/2021
    Description:
        Script usage (console): 
         - python etl.py
        Tasks:
         - Establish connection to sparkify database
         - Process the song and log data sets (in JSON) available at data/*
         - Creates and populates fact and dimension tables in star schema 
"""

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This method read song data set from filepath and populates the data into songs and artists tables

    Args:
        cur: Sparkify database cursor
        filepath: Path to the files containing song data set

    Returns: 
        None
    """

    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    artist_data = list(
        df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """
    This method reads log dataset from filepath and populates time, user and songsplay tables

    Args:
        cur: Sparkify database cursor
        filepath: Path to the files containing log data set

    Returns: 
        None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[(df.page == 'NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location,
        row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Get the data set files from filepath and invoke process_song_file or process_log_file

    Args:
        cur: Sparkify database cursor
        conn: Sparkify database connection
        filepath: Filepath for the data sets to be processed (song/log)
        func: Name of the method(s) to be invoked to process the data file and populate the schema

    Returns: 
        None
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    Driving method of pipeline that makes connection to database and invokes process_data to
    process log and song data sets

    Args:
        None

    Returns: 
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()