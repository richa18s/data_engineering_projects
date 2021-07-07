"""
    Author:
        RichaS
    Date Created:
        06/12/2021
    Description:
        Script usage (console): 
         - python etl.py
        Tasks:
         - Establish connection to AWS 
         - Process the song and log data sets (in JSON) available in s3 bucket
           and load the data in data frames and process data further
         - Create fact and dimension schemas in S3 bucket (at output location)
"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    This method create spark session
    Args:
        None
    Returns: 
        Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This method gets song data set from s3 bucket and creates dataframse for songs and artists. 
    Also writes the data from dataframes to output location.
    Args:
        spark: Spark session
        input_data: Path to input data
        output_data: Path to output data       
    Returns: 
        None
    """
    song_data = '{}{}'.format(input_data, 'song_data/*/*/*/*.json')
    
    df = spark.read.json(song_data)

    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    songs_table.printSchema()
    songs_table.write.partitionBy("year", "artist_id").parquet('{}songs/songs_table.parquet'.format(output_data))

    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", 
                        "artist_latitude as lattitude", "artist_longitude as longitude").distinct()
    
    artists_table.write.parquet('{}artists/artists_table.parquet'.format(output_data))


def process_log_data(spark, input_data, output_data):
    """
    This method gets log data set from s3 bucket and creates dataframes for users, time and songsplay.
    Also writes the data from dataframes to output location.
    Args:
        spark: Spark session
        input_data: Path to input data
        output_data: Path to output data 
    Returns: 
        None
    """
    log_data = '{}{}'.format(input_data, 'log_data/*/*/*.json')

    df = spark.read.json(log_data)
    
    df = df.filter(df.page == "NextSong")

    users = df.selectExpr("userId as user_id", "firstName as first_name", 
                          "lastName as last_name", "gender", "level").distinct()
    
    users.write.parquet('{}users/users.parquet'.format(output_data))

    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    df = df.withColumn("hour", hour(df.timestamp)) \
       .withColumn("month", month(df.timestamp)) \
       .withColumn("year", year(df.timestamp)) \
       .withColumn("week", weekofyear(df.timestamp)) \
       .withColumn("day", dayofmonth(df.timestamp)) \
       .withColumn("weekday", dayofweek(df.timestamp))
    
    time_table = df.selectExpr("timestamp as start_time", "hour" , 
                                       "day", "week", "month", "year", "weekday").distinct()

    time_table.write.partitionBy("year", "month").parquet('{}time/time_table.parquet'.format(output_data))

    songs_table = spark.read.parquet('{}songs/*/*/*'.format(output_data))
    artists_table = spark.read.parquet('{}artists/*'.format(output_data))
    
    song_df = df.join(songs_table, (songs_table.title == df.song) & (songs_table.duration == df.length)) \
            .join(artists_table, artists_table.name == df.artist) \
            .select(monotonically_increasing_id().alias("songplay_id"), 
                    df.timestamp, df.userId, df.level, songs_table.song_id, 
                    artists_table.artist_id, df.sessionId, df.location, 
                    df.userAgent, df.year, df.month)
    
    songplays_table = song_df.selectExpr("songplay_id", "timestamp as start_time", "userId as user_id", 
                             "level", "song_id", "artist_id", "sessionId as session_id", 
                             "location", "userAgent as user_agent", "year", "month").distinct()
    
    songplays_table.write.partitionBy("year", "month").parquet('{}songplays/songplays_table.parquet'.format(output_data))


def main():
    """
    Main method that invokes process_song_data, process_log_data 
    to process song_data and log_data datasets. 
    Args:
        None
    Returns: 
        None
    """
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://awsrichas/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
