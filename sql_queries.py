import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist          varchar,
        auth            varchar,
        firstName       varchar,
        gender          varchar,
        itemInSession   integer,
        lastName        varchar,
        length          FLOAT,
        level           varchar,
        location        text,
        method          varchar,
        page            varchar,
        registration    varchar,
        sessionId       integer,
        song            varchar,
        status          integer,
        ts              bigint,
        userAgent       varchar,
        userId          integer 
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id    BIGINT IDENTITY(0, 1),
        start_time     varchar  not null,
        user_id        varchar  not null,
        level          varchar  not null,
        song_id        varchar  not null,
        artist_id      varchar  not null,
        session_id     varchar  not null,
        location       varchar  not null,
        user_agent     varchar  not null,
        PRIMARY KEY (songplay_id)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id        varchar  not null,
        first_name     varchar,
        last_name      varchar,
        gender         varchar,
        level          varchar not null,
        PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id        varchar  not null,
        title          varchar,
        artist_id      varchar not null,
        year           integer,
        duration       numeric,
        PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id           varchar  not null,
        artist_name         varchar,
        artist_location     varchar,
        artist_latitude     numeric,
        artist_longitude    numeric,
        PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time    timestamp NOT NULL,
        hour          varchar,
        day           varchar,
        week          varchar,
        month         varchar,
        year          varchar,
        weekday       varchar,
        PRIMARY KEY (start_time)
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# INSERT QUERIES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  se.ts,
            se.userId,
            se.level,
            s.song_id,
            s.artist_id,
            se.sessionId,
            se.location,
            se.userAgent
    FROM staging_songs s
    JOIN staging_events se
    ON s.artist_name = se.artist
    AND s.title = se.song
    AND s.duration = se.length
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, 
                    title, 
                    artist_id, 
                    year, 
                    duration
    FROM staging_songs;
""")

artist_table_insert = ("""    
    INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT DISTINCT artist_id, 
                    artist_name, 
                    artist_location, 
                    artist_latitude, 
                    artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  ts.start_time,
            EXTRACT (HOUR FROM ts.start_time),
            EXTRACT (DAY FROM ts.start_time),
            EXTRACT (WEEK FROM ts.start_time), 
            EXTRACT (MONTH FROM ts.start_time),
            EXTRACT (YEAR FROM ts.start_time), 
            EXTRACT (WEEKDAY FROM ts.start_time) 
    FROM (
        SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM songplays
) ts;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
