from dagster import (
    asset,
    AssetIn,
    Output,
    StaticPartitionsDefinition,
)
from pyspark.sql import DataFrame
from datetime import datetime
import pyarrow as pa
import pandas as pd


@asset(
    ins={
        "gold_games": AssetIn(key_prefix=["football", "gold"]),
    },
    key_prefix=["warehouse", "analysis"],  # Changed from "warehouse" to "analysis"
    metadata={
        "description": "Warehouse layer: Games data stored in analysis schema",
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
    io_manager_key="psql_io_manager",
)
def games(context, gold_games: DataFrame) -> Output[pd.DataFrame]:
    games = gold_games.toPandas()
    return Output(
        value=games,
        metadata={
            "description": "Warehouse layer: Games data",
            "columns": list(games.columns),
            "records count": len(games),
        }
    )

@asset(
    ins={
        "gold_transfer": AssetIn(key_prefix=["football", "gold"]),
    },
    key_prefix=["warehouse", "analysis"],  # Changed from "warehouse" to "analysis"
    metadata={
        "description": "Warehouse layer: Games data stored in analysis schema",
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
    io_manager_key="psql_io_manager",
)
def transfer(context, gold_transfer: DataFrame) -> Output[pd.DataFrame]:
    transfer = gold_transfer.toPandas()
    return Output(
        value=transfer,
        metadata={
            "description": "Warehouse layer: Transfer data",
            "columns": list(transfer.columns),
            "records count": len(transfer),
        }
    )
@asset(
    ins={
        "gold_clubs": AssetIn(key_prefix=["football", "gold"]),
    },
    key_prefix=["warehouse", "analysis"],  # Changed from "warehouse" to "analysis"
    metadata={
        "description": "Warehouse layer: Clubs data stored in analysis schema",
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
    io_manager_key="psql_io_manager",
)
def clubs(context, gold_clubs: DataFrame) -> Output[pd.DataFrame]:
    clubs = gold_clubs.toPandas()
    return Output(
        value=clubs,
        metadata={
            "description": "Warehouse layer: Clubs data",
            "columns": list(clubs.columns),
            "records count": len(clubs),
        }
    )
@asset(
    ins={
        "gold_players": AssetIn(key_prefix=["football", "gold"]),
    },
    key_prefix=["warehouse", "analysis"],  # Changed from "warehouse" to "analysis"
    metadata={
        "description": "Warehouse layer: Players data stored in analysis schema",
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
    io_manager_key="psql_io_manager",
)
def players(context, gold_players: DataFrame) -> Output[pd.DataFrame]:
    players = gold_players.toPandas()
    return Output(
        value=players,
        metadata={
            "description": "Warehouse layer: Players data",
            "columns": list(players.columns),
            "records count": len(players),
        }
    )
@asset(
    ins={
        "gold_appearances": AssetIn(key_prefix=["football", "gold"]),
    },
    key_prefix=["warehouse", "analysis"],  # Changed from "warehouse" to "analysis"
    metadata={
        "description": "Warehouse layer: Appearances data stored in analysis schema",
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
    io_manager_key="psql_io_manager",
)
def appearances(context, gold_appearances: DataFrame) -> Output[pd.DataFrame]:
    appearances = gold_appearances.toPandas()
    return Output(
        value=appearances,
        metadata={
            "description": "Warehouse layer: Appearances data",
            "columns": list(appearances.columns),
            "records count": len(appearances),
        }
    )
