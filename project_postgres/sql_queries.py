# DROP TABLES

songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays
                             (songplay_id SERIAL PRIMARY KEY,  
                             start_time time REFERENCES time(start_time), 
                             user_id int REFERENCES users(user_id), 
                             level varchar,
                             song_id varchar REFERENCES songs(song_id), 
                             artist_id varchar REFERENCES artists(artist_id), 
                             session_id int NOT NULL, 
                             location varchar,
                             user_agent varchar)
""")


user_table_create = (""" CREATE TABLE IF NOT EXISTS users
                         (user_id int PRIMARY KEY, 
                         first_name varchar, 
                         last_name varchar, 
                         gender varchar, 
                         level varchar NOT NULL)
""")


song_table_create = (""" CREATE TABLE IF NOT EXISTS songs
                         (song_id varchar PRIMARY KEY, 
                         title varchar, 
                         artist_id varchar REFERENCES artists(artist_id), 
                         year int,
                         duration float)
""")


artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists
                           (artist_id varchar PRIMARY KEY, 
                           name varchar NOT NULL, 
                           location varchar, 
                           latitude float, 
                           longtitude float)
""")


time_table_create = (""" CREATE TABLE IF NOT EXISTS time
                         (start_time time PRIMARY KEY, 
                         hour int, 
                         day int, 
                         week int, 
                         month int, 
                         year int, 
                         weekday int)
""")
                         

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, 
                          song_id, artist_id, session_id, location, user_agent)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                          on conflict (songplay_id) do nothing
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
                         VALUES (%s, %s, %s, %s, %s)
                         on conflict (user_id) 
                         do update set level = EXCLUDED.level
""")


song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration) 
                          VALUES (%s, %s, %s, %s, %s)
                          on conflict (song_id) do nothing
""")

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longtitude) 
                          VALUES (%s, %s, %s, %s, %s)
                          on conflict (artist_id) do nothing
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                          VALUES (%s, %s, %s, %s, %s, %s, %s)
                          on conflict (start_time) do nothing
""")

# FIND SONGS

song_select = (""" Select a.song_id, b.artist_id 
                    from songs a join artists b 
                    on a.artist_id = b.artist_id
                    where a.title = %s 
                    and b.name = %s 
                    and a.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]