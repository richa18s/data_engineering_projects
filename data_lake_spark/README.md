<h4>PROJECT : DATA WAREHOUSE</h4>
Sparkify’s analytical team wants to analyze the data collected on songs and user activity on their new streaming app. They want to analyze what songs users are listening to. The user activity logs and song metadata is available to analyze for this specific use case.  <br><br>
The data is available in JSON format on S3 buckets, needs to be transformed using spark dataframes to be further structured and put into respective facts and dimension schemas in S3. ETL pipeline to be created to carry out the transformation. The project will use AWS, python, spark to create the pipeline.<br><br>


<h5>DataSets</h5> 
- Song Dataset:  Resides in s3://udacity-dend/song_data. Data files are in Json format and contains metadata about song and the artist of the song.  The files are partitioned by the first three letters of each song's track ID.<br>
- Log Dataset: Resides in s3://udacity-dend/log_data. Data files are in Json format and contains user activity.  The files are partitioned by partitioned by year and month<br><br>

<h5>AWS Redshift Cluster</h5> 
AWS Redshift is used in ETL pipeline <br>
- Cluster : EMR (m5.xlarge) <br>
- Number of nodes : 3 (1 master, 2 slaves)  <br>

<h5>Fact Schema</h5>
1. songsplay: log data associated with song play<br>
   - attributes:  songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent<br>


<h5>Dimension Schema:</h5>
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

The ETL pipeline contains following files to load the data into the respective schemas:<br>
1. dl.cfg : Contains secret id and key for IAM admin user to connect to AWS buckets.
3. etl.py: Script contains following methods:
   - create_spark_session: Creates and returns new spark session
   - process_song_data: This method gets song data set from s3 bucket and creates dataframse for songs and artists. Also writes the data from dataframes to output location. <br>
   - process_log_data: This method gets log, songs, artists data set from s3 bucket and creates dataframes for users, time and songsplay. Also writes the data from dataframes to output location. <br>
   - main: This method  invokes process_song_data, process_log_data to process song_data and log_data data sets
