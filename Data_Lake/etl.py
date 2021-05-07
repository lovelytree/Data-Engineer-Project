import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS","AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """
    Create a spark session and return it
    """
   
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song data:
    1) extract data from s3
    2) transform them to a set of dimensional tables 
    3) load tables back into S3 
    
    Parameters:
        spark: spark session
        input_data:  input path
        output_data: output path
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    print(song_data)
    
    # read song data file
    print("Read song data file from S3")
    df = spark.read.json(song_data)
 
    # extract columns to create songs table
    # songs table: song_id, title, artist_id, year, duration
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table/", 'overwrite')
    print("Write songs table completed !")

    # extract columns to create artists table
    # artists table: artist_id, name, location, lattitude, longitude
    artists_table = df.select("artist_id", 
                              col("artist_name").alias("name"), 
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("lattitude"), 
                              col("artist_longitude").alias("longitude")).dropDuplicates()
        
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table/",'overwrite')
    print("Write artists table completed !")


def process_log_data(spark, input_data, output_data):
    """
    Process the user log data: 
    1) extract data from s3
    2) transform them to a set of dimensional tables 
    3) load tables back into S3 
    
    Parameters:
        spark: spark session
        input_data:  input path
        output_data: output path
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    print(log_data)

    # read log data file
    print("Read log data file from S3")
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # create a user log data view 
    df.createOrReplaceTempView("log_v")
    
    # extract columns for users table 
    # user table: user_id, first_name, last_name, gender, level
    users_table = spark.sql("""
        SELECT DISTINCT userId as user_id, 
            firstName as first_name, 
            lastName as last_name, 
            gender, 
            level
        FROM log_v
        WHERE userId is NOT NULL
    """)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table/", 'overwrite')
    print("Write users table completed !")

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    # time table: start_time, hour, day, week, month, year, weekday
    time_table = spark.sql("""
        SELECT DISTINCT start_time,
            hour(start_time) as hour,
            dayofmonth(start_time) as day,
            weekofyear(start_time) as week,
            month(start_time) as month,
            year(start_time) as year,
            dayofweek(start_time) as weekday
        FROM log_v
        WHERE start_time is NOT NULL
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table/", 'overwrite')
    print("Write time table completed !")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")
    
    # create song data view
    song_df.createOrReplaceTempView("song_v")
    

    # extract columns from joined song and log datasets to create songplays table 
    # songplays table: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = spark.sql("""
        SELECT monotonically_increasing_id() as songplay_id,
            lv.start_time as start_time,
            lv.userId as user_id,
            lv.level as level,
            sv.song_id as song_id,
            sv.artist_id as artist_id,
            lv.sessionId as session_id,
            lv.location as location,
            lv.userAgent as user_agent,
            year(lv.start_time) as year,
            month(lv.start_time) as month
        FROM log_v lv
        JOIN song_v sv 
        ON lv.song == sv.title AND lv.artist = sv.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays_table/", 'overwrite')
    print("Write songplays table completed !")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output/"

    print("Processing song data...")
    process_song_data(spark, input_data, output_data)    
    
    print("Processing user log data...")
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
