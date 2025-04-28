from dagster import asset, Output, AssetIn
import pandas as pd
from ..resources.spark_io_manager import get_spark_session
import os
from pyspark.sql import SparkSession, DataFrame , Row
from pyspark.sql.functions import col, when, lit

@asset(
    ins={
        "silver_games": AssetIn(key_prefix=["football", "silver"]),
        "silver_club_names": AssetIn(key_prefix=["football", "silver"]),
        "silver_cups": AssetIn(key_prefix=["football", "silver"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind="spark",
    group_name="gold_layer",
    key_prefix=["football", "gold"],
    description="This asset fills the null club names in game data using the scraped club names.",
)

def gold_games(context, silver_games: DataFrame, silver_club_names: pd.DataFrame, silver_cups: DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    with get_spark_session(config) as spark:
        df_filled = silver_games.toPandas()
        club_names_df= silver_club_names.copy()
        df_filled['home_club_name'] = df_filled['home_club_name'].replace('', pd.NA)
        df_filled['away_club_name'] = df_filled['away_club_name'].replace('', pd.NA)
        context.log.info(f"Original null home_club_name: {df_filled['home_club_name'].isnull().sum()}")
        context.log.info(f"Original null away_club_name: {df_filled['away_club_name'].isnull().sum()}")
        # Merge for home clubs
        df_filled = df_filled.merge(
            club_names_df.rename(columns={'club_id': 'home_club_id', 'club_name': 'new_home_club_name'}),
            on='home_club_id',
            how='left'
        )
        # Only use the new name where the original is null
        df_filled['home_club_name'] = df_filled['home_club_name'].fillna(df_filled['new_home_club_name'])
        df_filled.drop('new_home_club_name', axis=1, inplace=True)

        # Merge for away clubs
        df_filled = df_filled.merge(
            club_names_df.rename(columns={'club_id': 'away_club_id', 'club_name': 'new_away_club_name'}),
            on='away_club_id',
            how='left'
        )
        # Only use the new name where the original is null
        df_filled['away_club_name'] = df_filled['away_club_name'].fillna(df_filled['new_away_club_name'])
        df_filled.drop('new_away_club_name', axis=1, inplace=True)
        context.log.info(f"After null home_club_name: {df_filled['home_club_name'].isnull().sum()}")
        context.log.info(f"After null away_club_name: {df_filled['away_club_name'].isnull().sum()}")
        result= spark.createDataFrame(df_filled)

        cups_selected = silver_cups.select("competition_id", "name")
        
        # Join with explicit column selection to avoid duplicates
        context.log.info(f"Before cups join - row count: {result.count()}")
        
        # Perform join with an alias to disambiguate the columns
        joined_df = result.join(
            cups_selected.alias("cups"),
            result["competition_id"] == cups_selected["competition_id"],
            "left"
        ).drop(cups_selected["competition_id"])  # Remove the duplicate competition_id
        return Output(
            value=joined_df,
            metadata={
                "description": "Gold layer: Games data with filled club names",
                "columns": joined_df.columns,
                "records count": joined_df.count(),
            }
        )
@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "gold"],
    metadata={
        "description": "Gold layer: Clubs data with filled club names",
    },
    ins={
        "silver_transfers": AssetIn(key_prefix=["football", "silver"]),
    },
    group_name="gold_layer",
    compute_kind="spark",
)
def gold_transfer(context, silver_transfers : DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),      
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    with get_spark_session(config) as spark:
        silver_transfers = silver_transfers.drop("position","country_of_birth","city_of_birth","country_of_citizenship")
        return Output(
            value=silver_transfers,
            metadata={
                "description": "Gold layer: Transfers data",
                "columns": silver_transfers.columns,
                "records count": silver_transfers.count(),
            }
        )
@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "gold"],
    metadata={
        "description": "Gold layer: Players data with filled club names",
    },
    ins={
        "silver_clubs": AssetIn(key_prefix=["football", "silver"]),
        "silver_cups": AssetIn(key_prefix=["football", "silver"]),
    },
    group_name="gold_layer",
    compute_kind="spark",
)
def gold_clubs(context, silver_clubs: DataFrame, silver_cups: DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    with get_spark_session(config) as spark:
        clubs= silver_clubs.drop("foreigners_number","national_team_players", "stadium_seats","last_season","net_transfer_record","url")
        
        cups_selected = silver_cups.select("competition_id", "name")
        
        # Join with explicit column selection to avoid duplicates
        context.log.info(f"clubs columns {clubs.columns}")
        
        # Perform join with an alias to disambiguate the columns
        joined_df = clubs.join(
            cups_selected.alias("cups"),
            clubs["competition_id"] == cups_selected["competition_id"],
            "left"
        ).drop(cups_selected["competition_id"])  # Remove the duplicate competition_id
        return Output(
            value=joined_df,
            metadata={
                "description": "Gold layer: Clubs data",
                "columns": joined_df.columns,
                "records count": joined_df.count(),
            }
        )
@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "gold"],
    metadata={
        "description": "Gold layer: Appearances data",
    },
    ins={
        "silver_appearances": AssetIn(key_prefix=["football", "silver"]),
    },
    group_name="gold_layer",
    compute_kind="spark",
)
def gold_appearances(context, silver_appearances: DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    with get_spark_session(config) as spark:
        appearances= silver_appearances.drop("url")
        return Output(
            value=appearances,
            metadata={
                "description": "Gold layer: Appearances data",
                "columns": appearances.columns,
                "records count": appearances.count(),
            }
        )
@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "gold"],
    metadata={
        "description": "Gold layer: Players data with filled club names",
    },
    ins={
        "silver_players": AssetIn(key_prefix=["football", "silver"]),
    },
    group_name="gold_layer",
    compute_kind="spark",
)
def gold_players(context, silver_players: DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    with get_spark_session(config) as spark:
       
        return Output(
            value=silver_players,
            metadata={
                "description": "Gold layer: Players data",
                "columns": silver_players.columns,
                "records count": silver_players.count(),
            }
        )
@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "gold"],
    metadata={
        "description": "Gold layer: Clubs data with filled club names",
    },
    ins={
        "silver_appearances": AssetIn(key_prefix=["football", "silver"]),
    },
    group_name="gold_layer",
    compute_kind="spark",
)
def gold_appearances(context, silver_appearances: DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    with get_spark_session(config) as spark:
        return Output(
            value=silver_appearances,
            metadata={
                "description": "Gold layer: Appearances data",
                "columns": silver_appearances.columns,
                "records count": silver_appearances.count(),
            }
        )