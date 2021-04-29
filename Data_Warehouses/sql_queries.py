import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
ARN = config.get("IAM_ROLE", "ARN")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# STAGING TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events ( 
    artist        varchar,
    auth          varchar,
    firstName     varchar,
    gender        char(1),
    itemInSession integer,
    lastName      varchar,
    length        float,
    level         varchar,
    location      varchar,
    method        varchar,
    page          varchar,
    registration  bigint,
    sessionId     bigint,
    song          varchar,
    status        varchar,
    ts            timestamp,
    userAgent     varchar,
    userId        integer );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs ( 
    num_songs        integer,
    artist_id        varchar,
    artist_latitude  float,
    artist_longitude float,
    artist_location  varchar,
    artist_name      varchar,
    song_id          varchar,
    title            varchar,
    duration         float,
    year             integer );
""")

# FACT TABLE
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays ( 
    songplay_id   integer   IDENTITY(0,1) PRIMARY KEY,
    start_time    timestamp  NOT NULL distkey sortkey, 
    user_id       integer NOT NULL, 
    level         varchar,
    song_id       varchar NOT NULL, 
    artist_id     varchar NOT NULL, 
    session_id    bigint, 
    location      varchar, 
    user_agent    varchar);
""")

# DIM TABLES
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users ( 
    user_id       integer PRIMARY KEY distkey, 
    first_name    varchar NOT NULL, 
    last_name     varchar NOT NULL, 
    gender        char(1), 
    level         varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs ( 
    song_id       varchar PRIMARY KEY, 
    song_name     varchar NOT NULL, 
    artist_id     varchar NOT NULL distkey, 
    year          integer, 
    duration      float); 
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists ( 
    artist_id     varchar PRIMARY KEY distkey, 
    name          varchar NOT NULL, 
    location      varchar, 
    latitude      float, 
    longitude     float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time ( 
    start_time    timestamp PRIMARY KEY distkey sortkey,  
    hour          integer, 
    day           integer, 
    week          integer, 
    month         integer, 
    year          integer, 
    weekday       integer);
""")

# LOAD DATA TO STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role {}
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    FORMAT AS json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role {}
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS json 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES
# LOAD DATA TO FACT AND DIM TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events se
JOIN staging_songs ss 
ON se.song = ss.title AND se.artist = ss.artist_name AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name,last_name,gender,level)
SELECT DISTINCT userId, 
    firstName, 
    lastName, 
    gender, 
    level
FROM staging_events
WHERE userId is NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT song_id,
    title, 
    artist_id, 
    year,
    duration
FROM staging_songs
WHERE song_id is NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id,
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM staging_songs
WHERE artist_id is NOT NULL
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT ts,
    EXTRACT(HOUR FROM ts),
    EXTRACT(DAY FROM ts),
    EXTRACT(WEEK FROM ts),
    EXTRACT(MONTH FROM ts),
    EXTRACT(YEAR FROM ts),
    EXTRACT(WEEKDAY FROM ts)
FROM staging_events
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

