import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    songs_table = spark.sql('''
                    SELECT DISTINCT song_id
                    , title
                    , artist_id
                    , year
                    , duration
                    FROM song_data 
                    ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, "songs/") , mode="overwrite",partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = spark.sql('''
                    SELECT DISTINCT artist_id
                    , artist_name as name
                    , artist_location as location
                    ,artist_latitude as latitude
                    , artist_longitude as longitude
                    FROM song_data ''')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artist/") , mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data_table")


    # extract columns for users table    
    users_table = spark.sql('''
                            SELECT DISTINCT ldt.userId as user_id, 
                            ldt.firstName as first_name,
                            ldt.lastName as last_name,
                            ldt.gender as gender,
                            ldt.level as level
                            FROM log_data_table ldt
                            WHERE ldt.userId IS NOT NULL
                        ''')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda epoch: datetime.fromtimestamp(epoch/1000),TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    #recover log_data_table with start_time column
    df.createOrReplaceTempView("log_data_table_2")
    
    # create datetime column from original timestamp column
    # not necesary with the get_timestamp implementation
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = spark.sql('''
                    SELECT DISTINCT start_time
                    , hour(start_time) as hour
                    , day(start_time) as day
                    , weekofyear(start_time) as week
                    , month(start_time) as month
                    , year(start_time) as year
                    , dayofweek(start_time) as weekday
                    FROM log_data_table_2
                        ''')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read.format("parquet").option("basePath", os.path.join(output_data, "songs/"))\
                   .load(os.path.join(output_data, "songs/*/*/"))
    #song_df temporary view                
    song_df.createOrReplaceTempView("song_table")


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
            SELECT row_number() over (ORDER BY ldt2.ts ASC) as songplay_id
            , ldt2.start_time as start_time
            , ldt2.userId as user_id
            , ldt2.level as level
            , st.song_id
            , st.artist_id
            , ldt2.sessionId as session_id
            , ldt2.location as location
            , ldt2.userAgent as user_agent
            , year(ldt2.start_time) as year
            , month(ldt2.start_time) as month
            FROM log_data_table_2 ldt2 left join song_table st 
            ON ldt2.length = st.duration AND ldt2.song = st.title''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays/") , mode="overwrite",partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://limon-y-chile-datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
