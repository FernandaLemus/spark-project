# spark-project

## Documentation 

This repository contains the following files:
1. etl.py  (ETL process that takes jsons files from s3 bucket and processing it with Spark , creating dimensional tables and sending it to another s3 bucket (limon-y-chile-datalake)[here](https://drive.google.com/file/d/1HYDY7cI60ZFWuz4baIOv6Y4gVeQo9xhi/view?usp=sharing). )
2. dl.cfg (AWS keys)


## On how to run this process
1. Write *run etl.py* in a python console. In this step the data is taken from s3 and loaded with pyspark into temporary views to create the dimensional tables described below. The outputs of this process are parquet files saved in another s3 bucket.


## About the relational model
5 tables were created to facilitate the extraction of information about user activity on Sparkify through queries. This information is structured with the idea of analyzing the listened songs by the users.

The principal output of this pipeline is the songplays table which incorporate information about the users and the song they've played, including the information of the artist of these songs. In this way we can querying all the information of interest from a single table.

You can find a brief description and paths of those tables with a diagram  here below

### songplays table: 
records in log data associated with song plays
songplay_id: song identification number
start_time: playback start time 
user_id: user (who played the song) identification number
level: user subscrption level (free/paid)
song_id: played song identification number
artist_id: artist identification number
session_id: user session identification number
location: artist location
user_agent: browser from which the user accesses

saved in s3://limon-y-chile-datalake/songplays/

### users table: 
users in the app
user_id: user identification number
first_name: user first name
last_name: user last name
gender: user gender (M/F)
level: level of user subscription (free/paid)

saved in s3://limon-y-chile-datalake/users/


### songs table: 
songs in music database
song_id: song identification number
title: title of the song
artist_id: artist identification number 
year: year of the song 
duration: duration of the song (seconds)

saved in s3://limon-y-chile-datalake/songs/

### artists table
artists in music database
artist_id, artist_name, artist_location, artist_latitude, artist_longitude 

saved in s3://limon-y-chile-datalake/artist/


### time
timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

saved in s3://limon-y-chile-datalake/time/


You can find a diagram for the relationship model [here](https://dbdiagram.io/d/5fe00df79a6c525a03bbc81f).
