from typing import Dict

import pyspark.sql.types as T

SONGS_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "num_songs": (T.IntegerType(), True),
    "artist_id": (T.StringType(), True),
    "artist_lattitude": (T.DoubleType(), True),
    "artist_longitude": (T.DoubleType(), True),
    "artist_location": (T.StringType(), True),
    "artist_name": (T.StringType(), True),
    "song_id": (T.StringType(), True),
    "title": (T.StringType(), True),
    "duration": (T.FloatType(), True),
    "year": (T.IntegerType(), True),
}
SONGS_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in SONGS_ON_LOAD_FIELDS.items()]
)

LOGS_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "artist": (T.StringType(), True),
    "auth": (T.StringType(), True),
    "firstName": (T.StringType(), True),
    "gender": (T.StringType(), True),
    "itemInSession": (T.IntegerType(), True),
    "lastName": (T.StringType(), True),
    "length": (T.FloatType(), True),
    "level": (T.StringType(), True),
    "location": (T.StringType(), True),
    "method": (T.StringType(), True),
    "page": (T.StringType(), True),
    "registration": (T.LongType(), True),
    "sessionId": (T.IntegerType(), True),
    "song": (T.StringType(), True),
    "status": (T.IntegerType(), True),
    "ts": (T.LongType(), True),
    "userAgent": (T.StringType(), True),
    "userId": (T.StringType(), True),
}
LOGS_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in LOGS_ON_LOAD_FIELDS.items()]
)

USERS_FIELDS: Dict[str, T.DataType] = {
    "user_id": (T.StringType(), False),
    "first_name": (T.StringType(), False),
    "last_name": (T.StringType(), True),
    "gender": (T.StringType(), True),
    "level": (T.StringType(), False),
}
USERS_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in USERS_FIELDS.items()]
)

SONGS_FIELDS: Dict[str, T.DataType] = {
    "song_id": (T.StringType(), False),
    "title": (T.StringType(), False),
    "artist_id": (T.StringType(), False),
    "year": (T.IntegerType(), True),
    "duration": (T.FloatType(), True),
}
SONGS_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in SONGS_FIELDS.items()]
)

ARTISTS_FIELDS: Dict[str, T.DataType] = {
    "artist_id": (T.StringType(), False),
    "name": (T.StringType(), False),
    "location": (T.StringType(), True),
    "lattitude": (T.DoubleType(), True),
    "longitude": (T.DoubleType(), True),
}
ARTISTS_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in ARTISTS_FIELDS.items()]
)

TIME_FIELDS: Dict[str, T.DataType] = {
    "start_time": (T.TimestampType(), False),
    "hour": (T.IntegerType(), False),
    "day": (T.IntegerType(), False),
    "week": (T.IntegerType(), False),
    "month": (T.IntegerType(), False),
    "year": (T.IntegerType(), False),
    "weekday": (T.IntegerType(), False),
}
TIME_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in TIME_FIELDS.items()]
)

SONGPLAYS_FIELDS: Dict[str, T.DataType] = {
    "songplay_id": (T.TimestampType(), False),
    "start_time": (T.TimestampType(), False),
    "month": (T.IntegerType(), False),
    "year": (T.IntegerType(), False),
    "user_id": (T.StringType(), False),
    "level": (T.StringType(), False),
    "song_id": (T.StringType(), False),
    "artist_id": (T.StringType(), False),
    "session_id": (T.IntegerType(), False),
    "location": (T.StringType(), True),
    "user_agent": (T.StringType(), True),
}
SONGPLAYS_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in SONGPLAYS_FIELDS.items()]
)

TABLES_SCHEMAS = {
    "users": USERS_SCHEMA,
    "songs": SONGS_SCHEMA,
    "artists": ARTISTS_SCHEMA,
    "time": TIME_SCHEMA,
    "songplays": SONGPLAYS_SCHEMA,
}
