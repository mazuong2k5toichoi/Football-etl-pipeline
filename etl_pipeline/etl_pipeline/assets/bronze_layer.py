from dagster import asset, Output
import pandas as pd

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    metadata={
        "description": "Bronze layer: Raw data from MySQL",
    },
    group_name="bronze_layer",
    compute_kind="MySQL",
)

def bronze_appearances(context) -> Output[pd.DataFrame]:
    sql= "SELECT * FROM appearances"
    pd_data = context.resources.mysql_io_manager.extract_data(sql)
    
    return Output(
        pd_data,
        metadata={
            "table": "appearances",
            "records count": len(pd_data),
            "columns": list(pd_data.columns),
        },
    )
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    metadata={
        "description": "Bronze layer: Raw data from MySQL",
    },
    group_name="bronze_layer",
    compute_kind="MySQL",
)
def bronze_players(context) -> Output[pd.DataFrame]:
    sql="SELECT * FROM players"
    pd_data= context.resources.mysql_io_manager.extract_data(sql)
    return Output(
        pd_data,
        metadata={
            "table": "players",
            "records count": len(pd_data),
            "columns": list(pd_data.columns),
        },
    )
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    metadata={
        "description": "Bronze layer: Raw data from MySQL",
    },
    group_name="bronze_layer",
    compute_kind="MySQL",
)
def bronze_games(context) -> Output[pd.DataFrame]:
    sql="SELECT * FROM games"
    pd_data= context.resources.mysql_io_manager.extract_data(sql)
    return Output(
        pd_data,
        metadata={
            "table": "games",
            "records count": len(pd_data),
            "columns": list(pd_data.columns),
        },
    )
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    metadata={
        "description": "Bronze layer: Raw data from MySQL",
    },
    group_name="bronze_layer",
    compute_kind="MySQL",
)
def bronze_competitions(context) -> Output[pd.DataFrame]:
    sql="SELECT * FROM competitions"
    pd_data= context.resources.mysql_io_manager.extract_data(sql)
    return Output(
        pd_data,
        metadata={
            "table": "competitions",
            "records count": len(pd_data),
            "columns": list(pd_data.columns),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    metadata={
        "description": "Bronze layer: Raw data from MySQL",
    },
    group_name="bronze_layer",
    compute_kind="MySQL",
)
def bronze_clubs(context) -> Output[pd.DataFrame]:
    sql="SELECT * FROM clubs"
    pd_data= context.resources.mysql_io_manager.extract_data(sql)
    return Output(
        pd_data,
        metadata={
            "table": "clubs",
            "records count": len(pd_data),
            "columns": list(pd_data.columns),
        },
    )
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    metadata={
        "description": "Bronze layer: Raw data from MySQL",
    },
    group_name="bronze_layer",
    compute_kind="MySQL",
)
def bronze_game_events(context) -> Output[pd.DataFrame]:
    sql="SELECT * FROM game_events"
    pd_data= context.resources.mysql_io_manager.extract_data(sql)
    return Output(
        pd_data,
        metadata={
            "table": "game_events",
            "records count": len(pd_data),
            "columns": list(pd_data.columns),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    metadata={
        "description": "Bronze layer: Raw data from MySQL",
    },
    group_name="bronze_layer",
    compute_kind="MySQL",
)
def bronze_transfers(context) -> Output[pd.DataFrame]:
    sql="SELECT * FROM transfers"
    pd_data= context.resources.mysql_io_manager.extract_data(sql)
    return Output(
        pd_data,
        metadata={
            "table": "transfers",
            "records count": len(pd_data),
            "columns": list(pd_data.columns),
        },
    )
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    metadata={
        "description": "Bronze layer: Raw data from MySQL",
    },
    group_name="bronze_layer",
    compute_kind="MySQL",
)
def bronze_game_lineups(context) -> Output[pd.DataFrame]:
    sql="SELECT * FROM game_lineups"
    pd_data= context.resources.mysql_io_manager.extract_data(sql)
    return Output(
        pd_data,
        metadata={
            "table": "game_lineups",
            "records count": len(pd_data),
            "columns": list(pd_data.columns),
        },
    )