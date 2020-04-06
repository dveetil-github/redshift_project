import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_IAM_ROLE_NAME = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")
DB_USER = config.get("CLUSTER", "DB_USER")
DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
HOST = config.get("CLUSTER", "HOST")
DB_PORT = config.get("CLUSTER", "DB_PORT")
DB_NAME = config.get("CLUSTER", "DB_NAME")

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS;"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS STAGING_EVENTS (
artist varchar, 
auth varchar,
firstName varchar,
gender varchar,
itemInSession integer,
lastName varchar,
length numeric,
level varchar,
location varchar,
method varchar,
page varchar,
registration numeric,
session_id integer,
song varchar,
status integer,
ts numeric,
userAgent varchar,
userId varchar
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS STAGING_SONGS (
song_id varchar, 
title varchar,
year integer, 
duration NUMERIC,
artist_id varchar, 
artist_latitude varchar,
artist_longitude varchar,
artist_location varchar,
artist_name varchar,
num_songs integer
);
""")

songplay_table_create = (""" create table if not exists songplays(
songplay_id bigint IDENTITY(0,1) , 
start_time varchar, 
user_id varchar, 
level varchar,
song_id varchar, 
artist_id varchar, 
session_id varchar, 
location varchar, 
user_agent varchar
);
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""create table if not exists time (
start_time varchar,
hour int,
day int,
week int,
month int,
year int,
weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json 'auto' compupdate off region 'us-west-2';
""").format(LOG_DATA, DWH_IAM_ROLE_NAME)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto' compupdate off region 'us-west-2';
""").format(SONG_DATA, DWH_IAM_ROLE_NAME)

# FINAL TABLES

songplay_table_insert = ("""
    select ts,userId,level,
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""insert into time (
start_time,hour,day,week,month,year,weekday) 
values(%s,%s,%s,%s,%s,%s,%s);
""")

# FIND SONGS
select_staging_events = ("""
select * from staging_events limit 10;
""")

song_select = ("""select song_id, artist_id from (select song_id,title,year,duration,name,a.artist_id,location from songs s, artists a where s.artist_id = a.artist_id ) sa where sa.title=%s and sa.name=%s and sa.duration=%s""")

# QUERY LISTS

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,time_table_drop]
#copy_table_queries = [staging_events_copy, staging_songs_copy]
copy_table_queries = [staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
