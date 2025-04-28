import os
from dagster import Definitions, file_relative_path, AssetKey
from .assets.bronze_layer import *
from .assets.silver_layer import *
from .assets.gold_layer import *
from .assets.warehouse import *
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from .resources.spark_io_manager import SparkIOManager
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD")
}
MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY")
}
PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}
SPARK_CONFIG = {
    "spark_master": os.getenv("SPARK_MASTER_URL"),
    "spark_version": os.getenv("SPARK_VERSION"),
    "hadoop_version": os.getenv("HADOOP_VERSION"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

DBT_PROJECT_PATH = file_relative_path(__file__, "../football_analysis")
DBT_PROFILES = file_relative_path(__file__, "../football_analysis/config")
dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
    key_prefix=["dbt"],
)
def flatten_assets(asset_lists):
    """
    Flattens a list that may contain both individual assets and lists of assets
    into a single flat list of assets.
    """
    result = []
    for item in asset_lists:
        if isinstance(item, list):
            result.extend(flatten_assets(item))
        else:
            result.append(item)
    return result
# First, create a list of all assets
assets_list = [
    bronze_appearances,
    bronze_players,
    bronze_games,
    bronze_competitions,
    bronze_clubs,
    bronze_game_events,
    bronze_transfers,
    bronze_game_lineups,
    silver_cups,
    silver_players,
    silver_transfers,
    silver_clubs,
    silver_games,
    silver_club_names,
    silver_appearances,
    gold_games,
    gold_clubs,
    gold_transfer,
    gold_appearances,
    gold_players,
    games,
    clubs,
    transfer,
    appearances,
    players,
    dbt_assets
]

# Use the custom flatten_assets function
flattened_assets = flatten_assets(assets_list)

# Additional validation to ensure we only have valid asset objects
valid_assets = []
for asset in flattened_assets:
    # Skip any None values or invalid assets
    if asset is not None:
        valid_assets.append(asset)

# Use validated assets
defs = Definitions(
    assets=valid_assets,
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
        "spark_io_manager": SparkIOManager(SPARK_CONFIG),
        "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        }),
    },
)
