
# ETL Processes
Use this notebook to develop the ETL process for each of your tables before completing the `etl.py` file to load the whole datasets.


```python
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
```


```python
conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()
```


```python
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files
```

# Process `song_data`
In this first part, you'll perform ETL on the first dataset, `song_data`, to create the `songs` and `artists` dimensional tables.

Let's perform ETL on a single song file and load a single record into each table to start.
- Use the `get_files` function provided above to get a list of all song JSON files in `data/song_data`
- Select the first song in this list
- Read the song file and view the data


```python
song_files = get_files("data/song_data")
```


```python
filepath = song_files[0]
```


```python
df = pd.read_json(filepath, lines=True)
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist_id</th>
      <th>artist_latitude</th>
      <th>artist_location</th>
      <th>artist_longitude</th>
      <th>artist_name</th>
      <th>duration</th>
      <th>num_songs</th>
      <th>song_id</th>
      <th>title</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ARD7TVE1187B99BFB1</td>
      <td>NaN</td>
      <td>California - LA</td>
      <td>NaN</td>
      <td>Casual</td>
      <td>218.93179</td>
      <td>1</td>
      <td>SOMZWCG12A8C13C480</td>
      <td>I Didn't Mean To</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



## #1: `songs` Table
#### Extract Data for Songs Table
- Select columns for song ID, title, artist ID, year, and duration
- Use `df.values` to select just the values from the dataframe
- Index to select the first (only) record in the dataframe
- Convert the array to a list and set it to `song_data`


```python
song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
song_data
```




    ['SOMZWCG12A8C13C480', "I Didn't Mean To", 'ARD7TVE1187B99BFB1', 0, 218.93179]



#### Insert Record into Song Table
Implement the `song_table_insert` query in `sql_queries.py` and run the cell below to insert a record for this song into the `songs` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `songs` table in the sparkify database.


```python
cur.execute(song_table_insert, song_data)
conn.commit()
```

Run `test.ipynb` to see if you've successfully added a record to this table.

## #2: `artists` Table
#### Extract Data for Artists Table
- Select columns for artist ID, name, location, latitude, and longitude
- Use `df.values` to select just the values from the dataframe
- Index to select the first (only) record in the dataframe
- Convert the array to a list and set it to `artist_data`


```python
artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
artist_data
```




    ['ARD7TVE1187B99BFB1', 'Casual', 'California - LA', nan, nan]



#### Insert Record into Artist Table
Implement the `artist_table_insert` query in `sql_queries.py` and run the cell below to insert a record for this song's artist into the `artists` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `artists` table in the sparkify database.


```python
cur.execute(artist_table_insert, artist_data)
conn.commit()
```

Run `test.ipynb` to see if you've successfully added a record to this table.

# Process `log_data`
In this part, you'll perform ETL on the second dataset, `log_data`, to create the `time` and `users` dimensional tables, as well as the `songplays` fact table.

Let's perform ETL on a single log file and load a single record into each table.
- Use the `get_files` function provided above to get a list of all log JSON files in `data/log_data`
- Select the first log file in this list
- Read the log file and view the data


```python
log_files = get_files("data/log_data")
```


```python
filepath = log_files[0]
```


```python
df = pd.read_json(filepath, lines=True)
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>auth</th>
      <th>firstName</th>
      <th>gender</th>
      <th>itemInSession</th>
      <th>lastName</th>
      <th>length</th>
      <th>level</th>
      <th>location</th>
      <th>method</th>
      <th>page</th>
      <th>registration</th>
      <th>sessionId</th>
      <th>song</th>
      <th>status</th>
      <th>ts</th>
      <th>userAgent</th>
      <th>userId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Stephen Lynch</td>
      <td>Logged In</td>
      <td>Jayden</td>
      <td>M</td>
      <td>0</td>
      <td>Bell</td>
      <td>182.85669</td>
      <td>free</td>
      <td>Dallas-Fort Worth-Arlington, TX</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540992e+12</td>
      <td>829</td>
      <td>Jim Henson's Dead</td>
      <td>200</td>
      <td>1543537327796</td>
      <td>Mozilla/5.0 (compatible; MSIE 10.0; Windows NT...</td>
      <td>91</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Manowar</td>
      <td>Logged In</td>
      <td>Jacob</td>
      <td>M</td>
      <td>0</td>
      <td>Klein</td>
      <td>247.56200</td>
      <td>paid</td>
      <td>Tampa-St. Petersburg-Clearwater, FL</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540558e+12</td>
      <td>1049</td>
      <td>Shell Shock</td>
      <td>200</td>
      <td>1543540121796</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>73</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Morcheeba</td>
      <td>Logged In</td>
      <td>Jacob</td>
      <td>M</td>
      <td>1</td>
      <td>Klein</td>
      <td>257.41016</td>
      <td>paid</td>
      <td>Tampa-St. Petersburg-Clearwater, FL</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540558e+12</td>
      <td>1049</td>
      <td>Women Lose Weight (Feat: Slick Rick)</td>
      <td>200</td>
      <td>1543540368796</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>73</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Maroon 5</td>
      <td>Logged In</td>
      <td>Jacob</td>
      <td>M</td>
      <td>2</td>
      <td>Klein</td>
      <td>231.23546</td>
      <td>paid</td>
      <td>Tampa-St. Petersburg-Clearwater, FL</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540558e+12</td>
      <td>1049</td>
      <td>Won't Go Home Without You</td>
      <td>200</td>
      <td>1543540625796</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>73</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Train</td>
      <td>Logged In</td>
      <td>Jacob</td>
      <td>M</td>
      <td>3</td>
      <td>Klein</td>
      <td>216.76363</td>
      <td>paid</td>
      <td>Tampa-St. Petersburg-Clearwater, FL</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540558e+12</td>
      <td>1049</td>
      <td>Hey_ Soul Sister</td>
      <td>200</td>
      <td>1543540856796</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>73</td>
    </tr>
  </tbody>
</table>
</div>



## #3: `time` Table
#### Extract Data for Time Table
- Filter records by `NextSong` action
- Convert the `ts` timestamp column to datetime
  - Hint: the current timestamp is in milliseconds
- Extract the timestamp, hour, day, week of year, month, year, and weekday from the `ts` column and set `time_data` to a list containing these values in order
  - Hint: use pandas' [`dt` attribute](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.html) to access easily datetimelike properties.
- Specify labels for these columns and set to `column_labels`
- Create a dataframe, `time_df,` containing the time data for this file by combining `column_labels` and `time_data` into a dictionary and converting this into a dataframe
df = df[(df.page == 'NextSong')]
df.head()

```python
t = pd.to_datetime(df['ts'],unit='ms')
t.head()
```




    0   2018-11-30 00:22:07.796
    1   2018-11-30 01:08:41.796
    2   2018-11-30 01:12:48.796
    3   2018-11-30 01:17:05.796
    4   2018-11-30 01:20:56.796
    Name: ts, dtype: datetime64[ns]




```python
time_data = (t.dt.time,t.dt.hour, t.dt.day, t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
```


```python
time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
time_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>start_time</th>
      <th>hour</th>
      <th>day</th>
      <th>week</th>
      <th>month</th>
      <th>year</th>
      <th>weekday</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>00:22:07.796000</td>
      <td>0</td>
      <td>30</td>
      <td>48</td>
      <td>11</td>
      <td>2018</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>01:08:41.796000</td>
      <td>1</td>
      <td>30</td>
      <td>48</td>
      <td>11</td>
      <td>2018</td>
      <td>4</td>
    </tr>
    <tr>
      <th>2</th>
      <td>01:12:48.796000</td>
      <td>1</td>
      <td>30</td>
      <td>48</td>
      <td>11</td>
      <td>2018</td>
      <td>4</td>
    </tr>
    <tr>
      <th>3</th>
      <td>01:17:05.796000</td>
      <td>1</td>
      <td>30</td>
      <td>48</td>
      <td>11</td>
      <td>2018</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01:20:56.796000</td>
      <td>1</td>
      <td>30</td>
      <td>48</td>
      <td>11</td>
      <td>2018</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



#### Insert Records into Time Table
Implement the `time_table_insert` query in `sql_queries.py` and run the cell below to insert records for the timestamps in this log file into the `time` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `time` table in the sparkify database.


```python
for i, row in time_df.iterrows():
    cur.execute(time_table_insert, list(row))
    conn.commit()
```

Run `test.ipynb` to see if you've successfully added records to this table.

## #4: `users` Table
#### Extract Data for Users Table
- Select columns for user ID, first name, last name, gender and level and set to `user_df`


```python
user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
user_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>userId</th>
      <th>firstName</th>
      <th>lastName</th>
      <th>gender</th>
      <th>level</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>91</td>
      <td>Jayden</td>
      <td>Bell</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>1</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>2</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>3</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>4</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>5</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>6</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>7</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>9</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>10</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>11</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>12</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>13</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>14</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>15</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>16</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>17</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>18</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>19</th>
      <td>73</td>
      <td>Jacob</td>
      <td>Klein</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>23</th>
      <td>86</td>
      <td>Aiden</td>
      <td>Hess</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>24</th>
      <td>86</td>
      <td>Aiden</td>
      <td>Hess</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>25</th>
      <td>86</td>
      <td>Aiden</td>
      <td>Hess</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>26</th>
      <td>86</td>
      <td>Aiden</td>
      <td>Hess</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>27</th>
      <td>86</td>
      <td>Aiden</td>
      <td>Hess</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>30</th>
      <td>24</td>
      <td>Layla</td>
      <td>Griffin</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>31</th>
      <td>24</td>
      <td>Layla</td>
      <td>Griffin</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>33</th>
      <td>24</td>
      <td>Layla</td>
      <td>Griffin</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>35</th>
      <td>24</td>
      <td>Layla</td>
      <td>Griffin</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>36</th>
      <td>24</td>
      <td>Layla</td>
      <td>Griffin</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>38</th>
      <td>24</td>
      <td>Layla</td>
      <td>Griffin</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>356</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>357</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>358</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>359</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>360</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>361</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>362</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>363</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>364</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>365</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>366</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>367</th>
      <td>91</td>
      <td>Jayden</td>
      <td>Bell</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>368</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>369</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>370</th>
      <td>91</td>
      <td>Jayden</td>
      <td>Bell</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>371</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>372</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>373</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>374</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>375</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>376</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>377</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>378</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>380</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>381</th>
      <td>49</td>
      <td>Chloe</td>
      <td>Cuevas</td>
      <td>F</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>382</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>383</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>384</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>385</th>
      <td>16</td>
      <td>Rylan</td>
      <td>George</td>
      <td>M</td>
      <td>paid</td>
    </tr>
    <tr>
      <th>387</th>
      <td>5</td>
      <td>Elijah</td>
      <td>Davis</td>
      <td>M</td>
      <td>free</td>
    </tr>
  </tbody>
</table>
<p>330 rows × 5 columns</p>
</div>



#### Insert Records into Users Table
Implement the `user_table_insert` query in `sql_queries.py` and run the cell below to insert records for the users in this log file into the `users` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `users` table in the sparkify database.


```python
for i, row in user_df.iterrows():
    cur.execute(user_table_insert, row)
    conn.commit()
```

Run `test.ipynb` to see if you've successfully added records to this table.

## #5: `songplays` Table
#### Extract Data and Songplays Table
This one is a little more complicated since information from the songs table, artists table, and original log file are all needed for the `songplays` table. Since the log file does not specify an ID for either the song or the artist, you'll need to get the song ID and artist ID by querying the songs and artists tables to find matches based on song title, artist name, and song duration time.
- Implement the `song_select` query in `sql_queries.py` to find the song ID and artist ID based on the title, artist name, and duration of a song.
- Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to `songplay_data`

#### Insert Records into Songplays Table
- Implement the `songplay_table_insert` query and run the cell below to insert records for the songplay actions in this log file into the `songplays` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `songplays` table in the sparkify database.
for index, row in df.iterrows():

    # get songid and artistid from song and artist tables
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()
    print(results)
    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None

    # insert songplay record
    songplay_data = (index, t.dt.time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
    cur.execute(songplay_table_insert, songplay_data)
    conn.commit()
Run `test.ipynb` to see if you've successfully added records to this table.

# Close Connection to Sparkify Database


```python
conn.close()
```

# Implement `etl.py`
Use what you've completed in this notebook to implement `etl.py`.


```python

```
