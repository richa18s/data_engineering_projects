<h4>PROJECT : DATA WAREHOUSE</h4>
Sparkify’s analytical team wants to analyze the data collected on songs and user activity on their new streaming app. They want to analyze what songs users are listening to. The user activity logs and song metadata is available to analyze for this specific use case.  <br><br>
The data is available in JSON format on S3 buckets, needs to be loaded into staging tables to be further structured and put into respective facts and dimension tables. ETL pipeline to be created to transfer the data from files into staging and from staging to respective facts and dimension tables of star schema of database that resides on AWS redshift cluster. The project will use AWS, python and SQL to create the pipeline.<br><br>


<h5>DataSets</h5> 
- Song Dataset:  Resides in s3://udacity-dend/song_data. Data files are in Json format and contains metadata about song and the artist of the song.  The files are partitioned by the first three letters of each song's track ID.<br>
- Log Dataset: Resides in s3://udacity-dend/log_data. Data files are in Json format and contains user activity.  The files are partitioned by partitioned by year and month<br><br>

<h5>AWS Redshift Cluster</h5> 
AWS Redshift is used in ETL pipeline as the database solution<br>
- Cluster : dc2.large nodes<br>
- Number of nodes : 4  <br>

<h5>Schema</h5>
- The star schema will consists of following tables:

<h5>Staging Tables</h5>
1. stg_event_data : Data dump from log dataset <br>
    - attributes : artist, auth, firstname, gender, iteminsession, lastname, length, level, location, method, page, registration, sessionid, song, status, ts, useragent, userid <br>
2. stg_song_data : Data dump from song dataset <br>
    - attributes : num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year
 

<h5>Fact Table</h5>
1. songsplay: log data associated with song play<br>
   - attributes:  songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent<br>


<h5>Dimension Tables:</h5>
1. users: list of users <br>
   - attributes : user_id, first_name, last_name, gender, level <br>
2. songs: songs in database <br>
   - attributes : song_id, title, artist_id, year, duration* <br>
3. artists: artist in database <br>
   - attributes: artist_id, name, location, latitude longitude* <br>
4. time: timestamp broken down to specific units <br>
   - attributes: start_time, hour, day, week, month, year, weekday* <br><br>


<h5>Project Design:</h5>

The log data from songs and user activity is analyzed and tables are designed  to ensure the specific query to  analyze the user activity gets the data from database in optimal time. 

The ETL pipeline contains following files to load the data into the respective tables:<br>
1. create_tables.py : Script contain methods to create staging, facts and dimension tables specified above.
2. sql_queries.py: Script contains the DDL and DML for stage, fact and dimension tables. This script is invoked by create_tables.py and etl.py
3. etl.py: Script contains following methods:
   - load_staging_tables: This method read song dataset and log dataset and load the data into staging tables <br>
   - insert_tables: This method populates songs, artists, time, user and songsplay tables <br>


Sample query that can be run after the star schema is populated using the pipeline:
Find songs  users have listened to in year 2018:<br>
>`select songplay_id, title from songsplay a join songs b on a.song_id = b. song_id join time c on a.start_time = c.start_time where c.year = 2018`