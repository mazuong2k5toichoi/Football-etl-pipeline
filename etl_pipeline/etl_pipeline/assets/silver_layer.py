from dagster import asset, Output, AssetIn
import pandas as pd
from ..resources.spark_io_manager import get_spark_session
from ..utils.club_scraper import ClubScraper

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

# We can remove the pandas_to_spark helper function since we'll use spark.createDataFrame directly


@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "silver"],
    metadata={
        "description": "Silver layer: Choose cups from bronze layer",
    },
    ins={
        "bronze_competitions": AssetIn(key_prefix=["football", "bronze"])
    },
    group_name="silver_layer",
    compute_kind="Pyspark",
)
def silver_cups(context, bronze_competitions: pd.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    
    with get_spark_session(config) as spark:
        df = spark.createDataFrame(bronze_competitions)
        
        cleaned_data = df.filter(col("competition_id").isin(["CL", "FAC", "USC", "EL", "L1", "ES1","FR1","GB1","IT1"])) \
            .drop("is_major_national_league", "competition_code", "sub_type", "country_id", "confederation")
        
        return Output(
            value=cleaned_data,
            metadata={
                "description": "Cleaned competitions data",
                "columns": cleaned_data.columns,
                "records count": cleaned_data.count(),
            }
        )

@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "silver"],
    metadata={
        "description": "Silver layer: Cleaned data from bronze layer",
    },
    ins={
        "bronze_players": AssetIn(key_prefix=["football", "bronze"])
    },
    group_name="silver_layer",
    compute_kind="Pyspark",
)
def silver_players(context, bronze_players: pd.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    
    with get_spark_session(config) as spark:
        bronze_players['hometown']=bronze_players['city_of_birth'] + ", " + bronze_players['country_of_birth']
        bronze_players.drop(['city_of_birth','country_of_birth'],axis=1,inplace=True)
        df = spark.createDataFrame(bronze_players)
        
        cleaned_data = df.select(
            col("player_id"),
            col("name").alias("player_name"),
            col("current_club_name"),
            col("date_of_birth"),
            col("position"),
            col("hometown"),
            col("foot"),
            col("height_in_cm"),
            col("image_url"),
            col("market_value_in_eur"),
            col("highest_market_value_in_eur")
        )
        
        row_count = cleaned_data.count()
        
        return Output(
            value=cleaned_data,
            metadata={
                "description": "Cleaned players data",
                "columns": cleaned_data.columns,
                "records count": row_count,
            }
        )
@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "silver"],
    metadata={
        "description": "Silver layer: Cleaned data from bronze layer",
    },
    ins={
        "bronze_transfers": AssetIn(key_prefix=["football", "bronze"]),
        "bronze_players": AssetIn(key_prefix=["football", "bronze"]),
    },
    group_name="silver_layer",
    compute_kind="Pyspark",
)
def silver_transfers(context, bronze_transfers: pd.DataFrame, bronze_players: pd.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    
    with get_spark_session(config) as spark:
        # Convert both dataframes to Spark DataFrames
        transfer_df = spark.createDataFrame(bronze_transfers).alias("transfers")
        player_df = spark.createDataFrame(bronze_players).alias("players")
        
        # Join DataFrames with explicit aliases to qualify columns
        transfer_history = transfer_df.join(
            player_df,
            on="player_id",
            how="left"
        )
        
        # Select required columns using qualified references when needed
        transfer_history = transfer_history.select(
            col("player_id"),
            col("transfers.player_name"),  # Specify which player_name if both DFs have it
            col("transfer_season"),
            col("from_club_name"),
            col("to_club_name"),
            col("transfers.market_value_in_eur").alias("transfer_market_value_in_eur"),
            col("name").alias("player_name_playerdf"),
            col("position"),
            col("country_of_birth"),
            col("city_of_birth"),
            col("country_of_citizenship")
        )
        
        return Output(
            value=transfer_history,
            metadata={
                "description": "Cleaned transfers data",
                "columns": transfer_history.columns,
            }
        )

@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "silver"],
    metadata={
        "description": "Silver layer: Cleaned data from bronze layer",
    },
    ins={
        "bronze_clubs": AssetIn(key_prefix=["football", "bronze"])
    },
    group_name="silver_layer",
    compute_kind="Pyspark",
)
def silver_clubs(context, bronze_clubs: pd.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    
    with get_spark_session(config) as spark:
        df = spark.createDataFrame(bronze_clubs)
        
        # Drop columns and rename 'name' to 'club_name'
        cleaned_data = df.drop("total_market_value", "coach_name", "filename", "club_code") \
            .withColumnRenamed("name", "club_name")
        cleaned_data = cleaned_data.filter(col("domestic_competition_id").isin(["CL", "FAC", "USC", "EL", "L1", "ES1","FR1","GB1","IT1"])) \
            .withColumnRenamed("domestic_competition_id", "competition_id")      

        row_count = cleaned_data.count()
        
        return Output(
            value=cleaned_data,
            metadata={
                "description": "Cleaned clubs data",
                "columns": cleaned_data.columns,
                "records count": row_count,
            }
        )
@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "silver"],
    metadata={
        "description": "Silver layer: Cleaned data from games",
    },
    ins={
        "bronze_games": AssetIn(key_prefix=["football", "bronze"])
    },
    group_name="silver_layer",
    compute_kind="Pyspark",
)
def silver_games(context, bronze_games: pd.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    
    with get_spark_session(config) as spark:
        df = spark.createDataFrame(bronze_games)
        
        # Drop columns and rename 'name' to 'club_name'
        cleaned_data = df.drop("home_club_position", "away_club_position", "aggregate", "competition_type") 
        
        cleaned_data = cleaned_data.filter(col("competition_id").isin(["CL", "FAC", "USC", "EL", "L1", "ES1","FR1","GB1","IT1"]))         
        row_count = cleaned_data.count()
        
        return Output(
            value=cleaned_data,
            metadata={
                "description": "Cleaned games data",
                "columns": cleaned_data.columns,
                "records count": row_count,
            }
        )
@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["football", "silver"],
    metadata={
        "description": "Silver layer: Club names scraped from Transfermarkt",
    },
    ins={
        "bronze_games": AssetIn(key_prefix=["football", "bronze"])
    },
    group_name="silver_layer",
    compute_kind="Pandas",
)
def silver_club_names(context, bronze_games: pd.DataFrame) -> Output[pd.DataFrame]:
    """Asset that scrapes missing club names from Transfermarkt"""
    context.log.info("Starting club name enrichment process")
    games_df = bronze_games.copy()

    # --- ADD THIS SECTION TO HANDLE EMPTY STRINGS ---
    context.log.info("Replacing empty strings with NaN in club name columns...")
    games_df['home_club_name'] = games_df['home_club_name'].replace('', pd.NA)
    games_df['away_club_name'] = games_df['away_club_name'].replace('', pd.NA)
    context.log.info(f"Null home_club_name count AFTER replacing empty strings: {games_df['home_club_name'].isnull().sum()}")
    context.log.info(f"Null away_club_name count AFTER replacing empty strings: {games_df['away_club_name'].isnull().sum()}")
    # --- END OF ADDED SECTION ---


    games_df = games_df.drop(columns=["home_club_position", "away_club_position", "aggregate", "competition_type"])
    games_df = games_df[games_df["competition_id"].isin(["CL", "FAC", "USC", "EL", "L1", "ES1","FR1","GB1","IT1"])]


    null_home_clubs = games_df[games_df['home_club_name'].isnull()]['home_club_id'].astype(int).unique()
    null_away_clubs = games_df[games_df['away_club_name'].isnull()]['away_club_id'].astype(int).unique()

    all_null_club_ids = pd.unique(pd.concat([
        pd.Series(null_home_clubs),
        pd.Series(null_away_clubs)
    ])).astype(int).tolist()

    context.log.info(f"Found {len(all_null_club_ids)} unique club IDs with missing names")

    # Initialize scraper with cache in the asset directory
    cache_dir = os.path.dirname(os.path.abspath(__file__))
    scraper = ClubScraper(cache_dir=cache_dir)

    # Scrape club names (5 concurrent requests)
    club_names_df = scraper.get_club_names_df(all_null_club_ids, max_workers=5)

    context.log.info(f"Successfully retrieved {len(club_names_df)} club names")

    return Output(
        club_names_df,
        metadata={
            "description": "Scraped club names",
            "records count": len(club_names_df),
            "columns": list(club_names_df.columns),
        }
    )
@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["football", "silver"],
    metadata={
        "description": "Silver layer: Cleaned data from game events",
    },
    ins={
        "bronze_appearances": AssetIn(key_prefix=["football", "bronze"]),
        "bronze_game_lineups": AssetIn(key_prefix=["football", "bronze"]),
    },
    group_name="silver_layer",
    compute_kind="Pyspark",
)
def silver_appearances(context, bronze_appearances: pd.DataFrame, bronze_game_lineups: pd.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    
    with get_spark_session(config) as spark:
        appearances_df = spark.createDataFrame(bronze_appearances)
        game_lineups_df = spark.createDataFrame(bronze_game_lineups)
        
        # Select needed columns and remove duplicates from game lineups
        game_lineups_cleaned = game_lineups_df.select(
            "game_id", "player_id", "type", "position", "number", "team_captain"
        ).distinct()
        
        # Log column names for debugging
        context.log.info(f"Appearances columns: {appearances_df.columns}")
        context.log.info(f"Game lineups columns: {game_lineups_cleaned.columns}")
        
        # Clean appearances data
        cleaned_appearances = appearances_df.drop("player_current_club_id", "date")
        cleaned_appearances = cleaned_appearances.filter(
            col("competition_id").isin(["CL", "FAC", "USC", "EL", "L1", "ES1", "FR1", "GB1", "IT1"])
        )
        
        # Join appearances with game lineups on game_id and player_id
        merged_df = cleaned_appearances.join(
            game_lineups_cleaned,
            on=["game_id", "player_id"],
            how="inner"
        )
        
        row_count = merged_df.count()
        context.log.info(f"Number of records in merged appearances data: {row_count}")
        
        return Output(
            value=merged_df,
            metadata={
                "description": "Cleaned and merged appearances data with lineup details",
                "columns": merged_df.columns,
                "records count": row_count,
            }
        )
