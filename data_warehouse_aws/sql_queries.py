import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS STG_EVENT_DATA"
staging_songs_table_drop = "DROP TABLE IF EXISTS STG_SONG_DATA"
songplay_table_drop = "DROP TABLE IF EXISTS FACT_SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS DIM_USERS"
song_table_drop = "DROP TABLE IF EXISTS DIM_SONGS"
artist_table_drop = "DROP TABLE IF EXISTS DIM_ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS DIM_TIME"



# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS STG_EVENT_DATA
(
    artist         varchar,
    auth           varchar,
    firstname      varchar,
    gender         varchar,
    iteminsession  integer,
    lastname       varchar,
    length         float,
    level          varchar,
    location       varchar,
    method         varchar,
    page           varchar,
    registration   bigint ,
    sessionid      integer,
    song           varchar,
    status         integer,
    ts             timestamp,
    useragent      varchar,
    userid         integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS STG_SONG_DATA
(
    num_songs       integer,
    artist_id       varchar,
    artist_latitude float, 
    artist_longitude float,
    artist_location varchar,
    artist_name     varchar,
    song_id         varchar,
    title           varchar,
    duration        float,
    year            int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS FACT_SONGPLAYS
   (songplay_id      integer   IDENTITY(0,1) PRIMARY KEY,  
    start_time       timestamp NOT NULL DISTKEY SORTKEY, 
    user_id          int       NOT NULL, 
    level            varchar   NOT NULL,
    song_id          varchar   NOT NULL, 
    artist_id        varchar   NOT NULL, 
    session_id       int, 
    location         varchar,
    user_agent       varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS DIM_USERS
    (user_id         int       SORTKEY PRIMARY KEY, 
     first_name      varchar   NOT NULL, 
     last_name       varchar   NOT NULL, 
     gender          varchar   NOT NULL, 
     level           varchar   NOT NULL);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS DIM_SONGS
     (song_id        varchar   SORTKEY PRIMARY KEY, 
      title          varchar   NOT NULL, 
      artist_id      varchar   NOT NULL, 
      year           int,
      duration       float);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS DIM_ARTISTS
     (artist_id      varchar   SORTKEY PRIMARY KEY, 
      name           varchar   NOT NULL, 
      location       varchar, 
      latitude       float, 
      longtitude     float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS DIM_TIME
     (start_time     timestamp SORTKEY DISTKEY PRIMARY KEY, 
      hour           int       NOT NULL, 
      day            int       NOT NULL, 
      week           int       NOT NULL, 
      month          int       NOT NULL, 
      year           int       NOT NULL, 
      weekday        int       NOT NULL);
""")



# STAGING TABLES
staging_events_copy = ("""
    copy STG_EVENT_DATA from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {}
    timeformat as 'epochmillisecs';
""").format(config.get('S3','LOG_DATA'), 
            config.get('IAM_ROLE','ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy STG_SONG_DATA from {} 
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto';
""").format(config.get('S3','SONG_DATA'), 
            config.get('IAM_ROLE','ARN'))



# FINAL TABLES
songplay_table_insert = ("""
    insert into fact_songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select e.ts, e.userid, e.level, s.song_id, s.artist_id, e.sessionid, e.location, e.useragent 
    from stg_event_data e, stg_song_data s
    where e.song = s.title and e.artist = s.artist_name
    and e.length = s.duration and e.page = 'NextSong';
""")

user_table_insert = ("""
    insert into dim_users 
    (user_id, first_name, last_name, gender, level)
    select DISTINCT userid, firstname, lastname, gender, level
    from stg_event_data 
    where page = 'NextSong';
""")

song_table_insert = ("""
    insert into dim_songs 
    (song_id, title, artist_id, year, duration)
    select song_id, title, artist_id, year, duration 
    from stg_song_data;
""")

artist_table_insert = ("""
    insert into dim_artists 
    (artist_id, name, location, latitude, longtitude)
    select artist_id, artist_name as name, nvl(artist_location, '') as location, 
    nvl(artist_latitude,0.0) as latitude, nvl(artist_longitude,0.0) as longtitude
    from stg_song_data;
""")

time_table_insert = ("""
    insert into dim_time
    (start_time, hour, day, week, month, year, weekday)
    select distinct ts, EXTRACT(hour from ts), EXTRACT(day from ts), EXTRACT(week from ts), 
    EXTRACT(month from ts),  EXTRACT(year from ts), EXTRACT(weekday from ts) 
    from stg_event_data; 
""")



# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [ staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
