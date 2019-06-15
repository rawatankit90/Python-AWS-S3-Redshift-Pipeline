import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(artist VARCHAR, auth VARCHAR,firstName VARCHAR, gender VARCHAR, itemInSession int,
lastName VARCHAR, length numeric, level VARCHAR, location VARCHAR, method VARCHAR,
page VARCHAR, registration VARCHAR, sessionId int, song VARCHAR, status int,
ts VARCHAR, userAgent VARCHAR, userId int)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (song_id varchar PRIMARY KEY,num_songs int,
title varchar, artist_name varchar, artist_latitude varchar, year int,
duration numeric NOT NULL,artist_id varchar, artist_longitude varchar,artist_location varchar)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(songplay_id int Identity(0,1) PRIMARY KEY, start_time timestamp not null,user_id varchar not null,
level varchar, song_id varchar not null,artist_id varchar not null, session_id varchar, location varchar,
user_agent varchar, CONSTRAINT song_artist_unq UNIQUE (song_id, artist_id))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id varchar PRIMARY KEY, first_name varchar NOT NULL,
last_name varchar NOT NULL, gender varchar,level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY,
title varchar NOT NULL,artist_id varchar not null ,year int,duration numeric NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY,
name varchar NOT NULL, location varchar, lattitude varchar, longitude varchar)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time varchar PRIMARY KEY, hour varchar,
day varchar , week varchar, month varchar, weekday varchar)
""")

# STAGING TABLES

staging_events_copy = ("""
copy {} from {}
credentials 'aws_iam_role={}'
json {}
compupdate off region 'us-west-2';
""").format("staging_events",LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
copy {} from {}
credentials 'aws_iam_role={}'
json 'auto'
compupdate off region 'us-west-2';
""").format("staging_songs",SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select distinct timestamp 'epoch' + ts/1000 * interval '1 second',userId,level,song_id,a.artist_id,
sessionId,stg.location,userAgent from artists a, songs s, staging_events stg
where a.name = stg.artist
and s.title = stg.song
and page='NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id,first_name,last_name,gender,level) (select distinct userid,firstName,
lastName,gender,level from staging_events where userid is not null and page='NextSong' )
""")

song_table_insert = ("""
INSERT INTO songs(song_id,title,artist_id,year,duration)
(select distinct song_id, title, artist_id, year, duration from staging_songs)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,lattitude,longitude) (select distinct artist_id,
artist_name, artist_location, artist_latitude, artist_longitude from staging_songs)
""")

# time_table_insert = ("""
# INSERT INTO time (start_time,hour,day,week,month,weekday)
# (select distinct timestamp 'epoch' + ts/1000 * interval '1 second',
# extract(hour from timestamp 'epoch' + ts/1000 * interval '1 second'),
# extract(day from timestamp 'epoch' + ts/1000 * interval '1 second'),
# extract(week from timestamp 'epoch' + ts/1000 * interval '1 second'),
# extract(month from timestamp 'epoch' + ts/1000 * interval '1 second'),
# extract(weekday from timestamp 'epoch' + ts/1000 * interval '1 second')
# from staging_events)
# """)

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,weekday)
(select start_time, extract(hour from start_time), extract (day from start_time),
extract(week from start_time), extract(month from start_time),
extract(weekday from start_time)  from songplays)
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,songplay_table_insert, time_table_insert]
