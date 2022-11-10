import logging
import warnings
from configparser import ConfigParser
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from rich import traceback

from table_schemas import LOGS_ON_LOAD_SCHEMA, SONGS_ON_LOAD_SCHEMA
from utils import create_s3_bucket, create_spark_session, process_config

_ = traceback.install()
logging.basicConfig(force=True)
logging.getLogger().setLevel(logging.INFO)
warnings.filterwarnings("ignore")


def create_songs_table(songs_on_load: DataFrame, save_path: str):
    """
    Create songs dimensional table from song data.

    Args:
        songs_on_load: song data.
        save_path: path to save songs table to.
    """
    logging.info("Creating songs table...")

    # 1. Extract columns to create songs table
    songs_table = (
        songs_on_load.select(["song_id", "title", "artist_id", "year", "duration"])
        .dropna(subset=["song_id", "artist_id"])
        .drop_duplicates(subset=["song_id"])
    )

    # 2. Write songs table to parquet files partitioned by year and artist
    partition_cols = ("year", "artist_id")
    songs_table.repartition(cols=partition_cols).write.parquet(
        save_path, partitionBy=partition_cols, mode="overwrite"
    )

    logging.info("Songs table successfully written.")


def create_artists_table(songs_on_load: DataFrame, save_path: str):
    """
    Create artists dimensional table from song data.

    Args:
        songs_on_load: song data.
        save_path: path to save artists table to.
    """
    logging.info("Creating artists table...")

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

    logging.info("Artists table successfully written.")


def process_song_data(spark: SparkSession, dl_config: ConfigParser):
    """
    Load song data from .json files and extract dimensional tables.

    Args:
        spark: Spark session.
        dl_config: data lake configuration parameters.
    """
    # 1. Load songs files
    logging.info("Loading song data...")
    songs_on_load = spark.read.option("recursiveFileLookup", "true").json(
        dl_config.get("S3", "SONG_DATA") + "/A/A/*/*.json", schema=SONGS_ON_LOAD_SCHEMA
    )
    logging.info("Song data successfully loaded.")

    # 2. Extract tables from songs data
    bucket_prefix = f"s3a://{dl_config.get('S3', 'DEST_BUCKET_NAME')}"
    create_songs_table(songs_on_load, f"{bucket_prefix}/songs")
    create_artists_table(songs_on_load, f"{bucket_prefix}/artists")


def create_users_table(logs_on_load: DataFrame, save_path: str):
    """
    Create users dimensional table from logs data.

    Args:
        logs_on_load: log data.
        save_path: path to save users table to.
    """
    logging.info("Creating users table...")

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

    logging.info("Users table successfully written.")


def create_time_table(logs_on_load: DataFrame, save_path: str):
    """
    Create time dimensional table from logs data.

    Args:
        logs_on_load: log data.
        save_path: path to save users table to.
    """
    logging.info("Creating time table...")

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
    partition_columns = ("year", "month")
    time_table.repartition(cols=partition_columns).write.parquet(
        save_path, partitionBy=partition_columns, mode="overwrite"
    )

    logging.info("Time table successfully written.")


def create_songplays_table(
    logs_on_load: DataFrame, songs_df: DataFrame, save_path: str
):
    """
    Create time dimensional table from logs data.

    Args:
        logs_on_load: log data.
        save_path: path to save users table to.
    """
    logging.info("Creating songplays table...")

    # 1. Extract columns to create songplays table
    songplays_cols_map = {
        "start_time": "timestamp",
        "user_id": "userId",
        "level": "level",
        "title": "song",
        "session_id": "sessionId",
        "location": "location",
        "user_agent": "userAgent",
    }
    songplays_table = logs_on_load.select(
        [F.col(c_old).alias(c_new) for c_new, c_old in songplays_cols_map.items()]
    )

    # 2. Get songplay_id column
    songplays_table = songplays_table.withColumn(
        "songplay_id", F.monotonically_increasing_id()
    )

    # 3. Join with songs dataframe to obtain missing columns
    songplays_table = songplays_table.join(
        songs_df.select(["title", "song_id", "artist_id"]), on="title"
    ).drop("title")

    # 4. Write songplays table to parquet files
    songplays_table.write.parquet(
        save_path, mode="overwrite"
    )

    logging.info("Songplays table successfully written.")


def process_log_data(spark: SparkSession, dl_config: ConfigParser):
    """
    Load log data from .json files and extract dimensional tables and one facts table.

    Args:
        spark: Spark session.
        dl_config: data lake configuration parameters.
    """
    # 1. Load songs files
    logs_on_load = spark.read.json(
        dl_config.get("S3", "LOG_DATA"), schema=LOGS_ON_LOAD_SCHEMA
    )

    # 2. Filter by actions for song plays
    logs_on_load = logs_on_load.where(logs_on_load.page == "NextSong")

    # 3. Get timestamp column
    logs_on_load = logs_on_load.withColumn(
        "timestamp", F.to_timestamp(logs_on_load.ts / 1000)
    )

    # 4. Create tables
    bucket_prefix = f"s3a://{dl_config.get('S3', 'DEST_BUCKET_NAME')}"
    create_users_table(logs_on_load, f"{bucket_prefix}/users")
    create_time_table(logs_on_load, f"{bucket_prefix}/time")

    songs_df = spark.read.parquet(f"{bucket_prefix}/songs")
    create_songplays_table(logs_on_load, songs_df, f"{bucket_prefix}/time")


def main():
    # 1. Read configuration files
    user_config, dl_config = (
        process_config(Path(__file__).parents[1].joinpath("_user.cfg")),
        process_config(Path(__file__).parents[1].joinpath("dl.cfg")),
    )

    # 2. Create destination bucket if it does not exist
    assert create_s3_bucket(user_config, dl_config), "Error creating S3 bucket."

    # 3. Create spark session
    spark = create_spark_session(user_config, dl_config)

    # 4. Process data
    process_song_data(spark, dl_config)
    process_log_data(spark, dl_config)


if __name__ == "__main__":
    main()
