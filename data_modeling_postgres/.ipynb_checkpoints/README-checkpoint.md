<h4>Project</h4>
Sparkify’s analytical team wants to analyze the data collected on songs and user activity on their new streaming app. They want to analyze what songs users are listening to. The user activity logs and song metadata is available to analyze for this specific use case. 
The data available in JSON format, needs to be structured and put into respective facts and dimension tables to be further analyzed. ETL pipeline to be created to transfer the data from files into facts and dimension tables of star schema of Postgres database. The project will use python and SQL to create the pipeline.<br><br>


<h5>DataSets</h5> 
- Song Dataset:  File in Json format and contains metadata about song and the artist of the song.  The files are partitioned by the first three letters of each song's track ID.<br>
- Log Dataset: File in Json format and contains user activity.  The files are partitioned by partitioned by year and month<br><br>


<h5>Database</h5>
- Postgres is being used for this project. The database is names as Sparkify.<br>


<h5>Schema</h5>
- The star schema will consists of following tables:


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

The log data from songs and user activity is analyzed and tables are designed to ensure the specific query to  analyze the user activity gets the data from database in optimal time. 

The ETL pipeline contains following files to load the data into the respective tables:<br>
1. create_tables.py : Script contain methods to create the facts and dimension tables specified above.
2. sql_queries.py: Script contains the DDL and DML for facts and dimension tables. This script is invoked by create_tables.py
3. etl.py: Script contains following methods:
   - process_song_file: This method read song dataset and populate songs and artists tables<br>
   - process_log_file: This method read log dataset and populate time, user and songsplay table<br>
   - process_data: Get the path for datasets and invoke process_song_file and process_log_file


Sample query that can be run after the star schema is populated using the pipeline:
Find songs  users have listened to in year 2018:<br>
>`select songplay_id, title from songsplay a join songs b on a.song_id = b. song_id join time c on a.start_time = c.start_time where c.year = 2018`