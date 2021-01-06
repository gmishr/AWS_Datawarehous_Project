import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
'''
Below we have sql queries to drop existing table to reset tables.
'''
staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS sparkify_user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

'''
Below queries are written to create stagging tables and final tables
'''
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stg_events(
artist          VARCHAR(300),
auth            VARCHAR(100),
first_name      VARCHAR(300),
gender          VARCHAR(5),
item_in_session INTEGER,
last_name       VARCHAR(300),
length          DECIMAL(9,5),
level           VARCHAR(50),
location        VARCHAR(300),
method          VARCHAR(50),
page            VARCHAR(50),
registration    DECIMAL(14,1),
session_id      INTEGER,
song            VARCHAR(300),
status          INTEGER,
ts              BIGINT,
user_agent      VARCHAR(300),
user_id         VARCHAR(10));
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_songs(
num_songs        INTEGER,
artist_id        VARCHAR(25), 
artist_latitude  DECIMAL(10, 5),
artist_longitude DECIMAL(10, 5),
artist_location  VARCHAR(300),
artist_name      VARCHAR(300),
song_id          VARCHAR(25),
title            VARCHAR(300),
duration         DECIMAL(9, 5),
year             INTEGER); 
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY NOT NULL,
start_time  TIMESTAMP NOT NULL SORTKEY,
user_id     VARCHAR(10),
level       VARCHAR(50),
song_id     VARCHAR(50) NOT NULL,
artist_id   VARCHAR(50) NOT NULL,
session_id  INTEGER,
location    VARCHAR(300),
user_agent  VARCHAR(300) );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS sparkify_user(
user_id     VARCHAR(10) PRIMARY KEY,
first_name  VARCHAR(300),
last_name   VARCHAR(300),
gender      VARCHAR(5),
level       VARCHAR(50));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(
song_id    VARCHAR(50) PRIMARY KEY NOT NULL,
title      VARCHAR(300) NOT NULL,
artist_id  VARCHAR(50) NOT NULL,
year       INTEGER,
duration   DECIMAL(9,5) NOT NULL)diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
artist_id      VARCHAR(50) PRIMARY KEY NOT NULL,
artist_name    VARCHAR(300) NOT NULL,
location       VARCHAR(300),
lattitude      DECIMAL(10,5),
longitude      DECIMAL(10,5))diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP  PRIMARY KEY NOT NULL SORTKEY,
hour       INTEGER NOT NULL,
day        INTEGER NOT NULL,
week       INTEGER NOT NULL,
month      INTEGER NOT NULL,
year       INTEGER NOT NULL,
weekday    INTEGER NOT NULL)diststyle all;
""")

# STAGING TABLES

'''
from below two copy script we are loading data from 
file placed in S3 Bucket to redshift stagging table. 
'''
staging_events_copy = ("""
copy stg_events

from {}

iam_role {}

region 'us-west-2'

format as  JSON {};
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE','ARN'),
			config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy stg_songs from {} 

iam_role {}

FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
"""
In below queries we are loading final redshift tables 
from stagging redshift tables.
"""


songplay_table_insert = ("""
INSERT INTO songplay(start_time,
                     user_id,
					 level,
					 song_id,
					 artist_id,
					 session_id,
					 location,
					 user_agent)
SELECT 
TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_time,
se.user_id,
se.level,
ss.song_id,
ss.artist_id,
se.session_id,
se.location,
se.user_agent
FROM stg_events se
JOIN stg_songs ss 
ON se.artist = ss.artist_name AND se.length = ss.duration AND se.song = ss.title
where se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO sparkify_user(user_id,
                          first_name,
						  last_name,
						  gender,
						  level)

SELECT DISTINCT
user_id,
first_name,
last_name,
gender,
level
FROM stg_events se
WHERE  user_id NOT IN (SELECT  
                       user_id 
					   FROM stg_events se2 
					   WHERE  se.user_id = se2.user_id AND se.ts < se2.ts )                        
""")

song_table_insert = ("""
INSERT INTO song(song_id,
                 title,
				 artist_id,
				 year,
				 duration)
SELECT DISTINCT
song_id,
title,
artist_id,
CASE WHEN year = 0 then NULL ELSE year END AS year,
duration
FROM stg_songs
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id,
                    artist_name,
					location,
					lattitude,
					longitude)
SELECT DISTINCT
artist_id,
artist_name,
artist_location as location,
artist_latitude as lattitude,
artist_latitude as longitude
FROM stg_songs 
""")

time_table_insert = ("""
INSERT INTO time(start_time,
                 hour,
				 day,
				 week,
				 month,
				 year,
				 weekday)
select 
t1.start_time,
EXTRACT(hour FROM t1.start_time) AS hour,
EXTRACT(day FROM t1.start_time) AS day,
EXTRACT(week FROM t1.start_time) AS week,
EXTRACT(month FROM t1.start_time) AS month,
EXTRACT(year FROM t1.start_time) AS year,
EXTRACT(weekday FROM t1.start_time) as weekday
FROM 
 (
   SELECT distinct
   TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' AS start_time
   FROM stg_events se
 ) t1 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
