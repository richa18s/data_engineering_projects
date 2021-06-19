<h4>PROJECT : DATA PIPELINES WITH AIRFLOW</h4>
Sparkify’s analytical team wants to analyze the data collected on songs and user activity on their new streaming app. They want to analyze what songs users are listening to. The user activity logs and song metadata is available to analyze for this specific use case.  <br><br>
The data is available in JSON format on S3 buckets, needs to be transformed using apache airflow dags to be further structured and put into respective facts and dimension tables in redshift. ETL pipeline to be created to carry out the transformation. The project will use AWS, python, sql, apache airflow to create the pipeline.<br><br>


<h5>DataSets</h5> 
- Song Dataset:  Resides in s3://udacity-dend/song_data. Data files are in Json format and contains metadata about song and the artist of the song.  The files are partitioned by the first three letters of each song's track ID.<br>
- Log Dataset: Resides in s3://udacity-dend/log_data. Data files are in Json format and contains user activity.  The files are partitioned by partitioned by year and month<br><br>

<h5>AWS Redshift Cluster</h5> 
AWS Redshift is used in ETL pipeline <br>
- Cluster : dc2.large nodes <br>
- Number of nodes : 4  <br>

<h5>Schema</h5>
- The star schema will consists of following tables:

<h5>Staging Tables</h5>
1. staging_events : Data dump from log dataset <br>
    - attributes : artist, auth, firstname, gender, iteminsession, lastname, length, level, location, method, page, registration, sessionid, song, status, ts, useragent, userid <br>
2. staging_songs : Data dump from song dataset <br>
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

The log data from songs and user activity is analyzed and tables are designed  to ensure the queries to  analyze the user activities gets the data from database in optimal time. 

Project Structure:

![alt text](images/tree_2.png "Title")


1. dags: This directory contains the python files that define the workflow
    - udac_example_dag.py: Script that has the tasks and workflow for the ETL pipepline. <br><br>

2. plugins: 
    1. helpers: This directory contains the helper sql queries 
        - sql_queries.py : sqls to populate fact and dimension tables 
    2. operators: This directory contains the use defined operators 
        - create_tables.py : Defines "CreateTableOperator" to help create the tables in redshift
        - data_quality.py : Defines "DataQualityOperator" to help check the quality of the data after the tables are populated in star schema
        - load_dimension.py: Defines "LoadDimensionOperator" to help load the data from staging to dimension tables
        - load_fact.py: Defines "LoadFactOperator" to help load the data from staging to fact table
        - stage_redshift: Defines "StageToRedshiftOperator" to help load data from S3 to staging tables.<br><br>

3. create_tables.sql : Script that contains SQL commands to DROP and CREATE the tables in star schema (staging, fact and dimension tables mentioned above)<br><br>


<h5>Airflow Pipeline:<h5>

![alt text](images/list.png "Title")
![alt text](images/tree.png "Title")
![alt text](images/graph.png "Title")