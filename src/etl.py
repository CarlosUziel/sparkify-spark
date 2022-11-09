import configparser
import os
from typing import Iterable, Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from table_schemas import LOGS_ON_LOAD_SCHEMA, SONGS_ON_LOAD_SCHEMA

config = configparser.ConfigParser()
config.read("_user.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def create_songs_table(songs_on_load: DataFrame, save_path: str):
    """
    Create songs dimensional table from song data.

    Args:
        songs_on_load: song data.
        save_path: path to save songs table to.
    """
    # 1. Extract columns to create songs table
    songs_table = (
        songs_on_load.select(["song_id", "title", "artist_id", "year", "duration"])
        .dropna(subset=["song_id", "artist_id"])
        .drop_duplicates(subset=["song_id"])
    )

    # 2. Write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        save_path, partitionBy=("year", "artist_id"), mode="overwrite"
    )


def create_artists_table(songs_on_load: DataFrame, save_path: str):
    """
    Create artists dimensional table from song data.

    Args:
        songs_on_load: song data.
        save_path: path to save artists table to.
    """
    # 0. Define artists column names mapping
    artists_cols_map = {
        "artist_id": "artist_id",
        "artist_name": "name",
        "artist_location": "location",
        "artist_lattitude": "lattitude",
        "artist_longitude": "longitude",
    }

    # 1. Subset songs data to get artists table
    artists_table = (
        songs_on_load.select(
            [F.col(c).alias(c_new) for c, c_new in artists_cols_map.items()]
        )
        .dropna(subset=["artist_id"])
        .drop_duplicates(subset=["artist_id"])
    )

    # 2. Write artists table to parquet files
    artists_table.write.parquet(save_path, mode="overwrite")


def process_song_data(
    spark: SparkSession, load_path: Union[str, Iterable[str]], save_root: str
):
    """
    Load song data from .json files and extract dimensional tables.

    Args:
        spark: Spark session.
        load_path: root path of song .json files.
        save_root: root path to store tables.
    """
    # 1. Load songs files
    songs_on_load = spark.read.json(load_path, schema=SONGS_ON_LOAD_SCHEMA)

    # 2. Extract tables from songs data
    create_songs_table(songs_on_load, f"{save_root}/songs")
    create_artists_table(songs_on_load, f"{save_root}/artists")


def create_users_table(logs_on_load: DataFrame, save_path: str):
    """
    Create users dimensional table from logs data.

    Args:
        logs_on_load: log data.
        save_path: path to save users table to.
    """
    # 0. Define artists column names mapping
    users_cols_map = {
        "user_id": "userId",
        "first_name": "firstName",
        "last_name": "lastName",
        "gender": "gender",
        "level": "level",
    }
    # 1. Extract columns to create songs table
    users_table = (
        logs_on_load.select(
            [F.col(c_old).alias(c_new) for c_new, c_old in users_cols_map.items()]
        )
        .dropna(subset=["user_id"])
        .drop_duplicates(subset=["user_id"])
    )

    # 2. Write users table to parquet files
    users_table.write.parquet(save_path, mode="overwrite")


def create_time_table(logs_on_load: DataFrame, save_path: str):
    """
    Create time dimensional table from logs data.

    Args:
        logs_on_load: log data.
        save_path: path to save users table to.
    """
    # 1. Extract columns to create time table
    time_table = logs_on_load.select(F.col("timestamp").alias("start_time"))

    time_funcs = {
        "hour": F.hour,
        "day": F.dayofmonth,
        "week": F.weekofyear,
        "month": F.month,
        "year": F.year,
        "weekday": F.dayofweek,
    }

    for time_unit, func in time_funcs.items():
        time_table = time_table.withColumn(time_unit, func(time_table.start_time))

    # 2. Write time table to parquet files partitioned by year and month
    time_table.write.parquet(save_path, partitionBy=("year", "month"), mode="overwrite")


def create_songplays_table(
    logs_on_load: DataFrame, songs_df: DataFrame, save_path: str
):
    """
    Create time dimensional table from logs data.

    Args:
        logs_on_load: log data.
        save_path: path to save users table to.
    """
    # 1. Extract columns to create songplays table
    songlplays_cols_map = {
        "start_time": "timestamp",
        "user_id": "userId",
        "level": "level",
        "title": "song",
        "session_id": "sessionId",
        "location": "location",
        "user_agent": "userAgent",
    }
    songplays_table = logs_on_load.select(
        [F.col(c_old).alias(c_new) for c_new, c_old in songlplays_cols_map.items()]
    )

    # 2. Get songplay_id column
    songplays_table = songplays_table.withColumn(
        "songplay_id", F.monotonically_increasing_id()
    )

    # 3. Join with songs dataframe to obtain missing columns
    songplays_table = songplays_table.join(
        songs_df.select(["title", "song_id", "artist_id"]), on="title"
    ).drop("title")

    # 4. Write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        save_path, partitionBy=("year", "month"), mode="overwrite"
    )


def process_log_data(
    spark: SparkSession, load_path: Union[str, Iterable[str]], save_root: str
):
    """
    Load log data from .json files and extract dimensional tables and one facts table.

    Args:
        spark: Spark session.
        load_path: root path of song .json files.
        save_root: root path to store tables.
    """
    # 1. Load songs files
    logs_on_load = spark.read.json(load_path, schema=LOGS_ON_LOAD_SCHEMA)

    # 2. Filter by actions for song plays
    logs_on_load = logs_on_load.where(logs_on_load.page == "NextSong")

    # 3. Get timestamp column
    logs_on_load = logs_on_load.withColumn(
        "timestamp", F.to_timestamp(logs_on_load.ts / 1000)
    )

    # 4. Create users table
    create_users_table(logs_on_load, f"{save_root}/users")

    # 5. Create time table
    create_time_table(logs_on_load, f"{save_root}/time")

    # 6. Create songplays table
    songs_df = spark.read.parquet(f"{save_root}/songs")
    create_songplays_table(logs_on_load, songs_df, f"{save_root}/time")


def main():
    spark = create_spark_session()
    output_data = "s3a://..."

    process_song_data(spark, "s3a://udacity-dend/song_data", output_data)
    process_log_data(spark, "s3a://udacity-dend/log_data", output_data)


if __name__ == "__main__":
    main()
