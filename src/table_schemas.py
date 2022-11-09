from typing import Any, Dict

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

LOGS_ON_LOAD_FIELDS: Dict[str, Any] = {
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
