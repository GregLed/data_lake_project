import configparser
from datetime import datetime
import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StringType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create and return spark session instance

    Returns
    -------
    pyspark.sql.SparkSession
        spark session instance to be used for ETL
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load songs data from S3 bucket, extract relevant data attributes
    for analytical tables (songs and artists) and store it back 
    on S3 bucket using parquet format.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        [description]
    input_data : str
        url to AWS S3 bucket with the raw data
    output_data : str
        url to AWS S3 bucket where output data will be stored
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    output_path_songs = os.path.join(output_data, 'songs.parquet')

    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(output_path_songs, 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                            'artist_latitude', 'artist_longitude') \
                        .withColumnRenamed('artist_name', 'name') \
                        .withColumnRenamed('artist_location', 'location') \
                        .withColumnRenamed('artist_latitude', 'latitude') \
                        .withColumnRenamed('artist_longitude', 'longitude') \
                        .dropDuplicates()
    
    # write artists table to parquet files
    output_path_artists = os.path.join(output_data, 'artists.parquet')
    artists_table.write.parquet(output_path_artists, 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Load logs and songs data from S3 bucket, extract relevant data attributes
    for analytical tables (users, time and songplays) and store it back 
    on S3 bucket using parquet format.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        [description]
    input_data : str
        url to AWS S3 bucket with the raw data
    output_data : str
        url to AWS S3 bucket where output data will be stored
    """

    # get filepath to log data files and song files
    log_data = input_data + 'log_data/*/*/*.json'
    song_data = input_data + 'song_data/*/*/*/*.json' 

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level') \
                    .withColumnRenamed('userId', 'user_id') \
                    .withColumnRenamed('firstName', 'first_name') \
                    .withColumnRenamed('lastName', 'last_name') \
                    .dropDuplicates()
    
    # write users table to parquet files
    output_path_users = os.path.join(output_data, 'users.parquet')
    users_table.write.parquet(output_path_users, 'overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', to_timestamp(col('ts')/1000))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(x / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('timestamp') \
                .withColumn('hour', hour('timestamp')) \
                .withColumn('day', dayofmonth('timestamp')) \
                .withColumn('week', weekofyear('timestamp')) \
                .withColumn('month', month('timestamp')) \
                .withColumn('year', year('timestamp')) \
                .withColumn('weekday', dayofweek('timestamp')) \
                .withColumnRenamed('timestamp', 'start_time') \
                .dropDuplicates()
        
    # write time table to parquet files partitioned by year and month
    output_path_time = os.path.join(output_data, 'time.parquet')
    time_table.write.partitionBy('year', 'month').parquet(output_path_time, 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    add_uuid = udf(lambda : str(uuid.uuid4()),StringType())

    df_joined = df.join(song_df, 
                        (df.artist == song_df.artist_name) & (df.song == song_df.title),
                        "inner")

    songplays_table = df_joined.select('timestamp', 'userId', 'level', 
                    'song_id', 'artist_id', 'sessionId', 
                    'location', 'userAgent') \
            .withColumnRenamed('timestamp', 'start_time') \
            .withColumnRenamed('userId', 'user_id') \
            .withColumnRenamed('sessionId', 'session_id') \
            .withColumnRenamed('userAgent', 'user_agent') \

    songplays_table = songplays_table.withColumn("songplay_id", add_uuid()) \
                    .withColumn('month', month('start_time')) \
                    .withColumn('year', year('start_time')) 

    # write songplays table to parquet files partitioned by year and month
    output_path_songplays = os.path.join(output_data, 'songplays.parquet')
    songplays_table.write.partitionBy('year', 'month').parquet(output_path_songplays, 'overwrite')

def main():
    """
    Run the program with the following steps:
    1. Create spark sessions
    2. ETL songs raw data
    3. ETL logs raw data
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifygreg/data_lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()