from contextlib import contextmanager
from sqlalchemy import create_engine
import pandas as pd
from dagster import IOManager, InputContext, OutputContext
from datetime import datetime
import psycopg2
from psycopg2 import sql
import psycopg2.extras

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        
    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass
    
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        with connect_psql(self._config) as conn:
            # Get columns to use
            ls_columns = (context.metadata or {}).get("columns", list(obj.columns))
            # Ensure all columns in ls_columns exist in the DataFrame
            valid_columns = [col for col in ls_columns if col in obj.columns]
            if not valid_columns:
                valid_columns = list(obj.columns)  # Use all columns if none specified are valid
            
            # Create connection for raw SQL execution
            conn_url = (
                f"postgresql://{self._config['user']}:{self._config['password']}"
                f"@{self._config['host']}:{self._config['port']}/{self._config['database']}"
            )
            with psycopg2.connect(conn_url) as raw_conn, raw_conn.cursor() as cursor:
                # Check if table exists
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (schema, table))
                
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    # Use TRUNCATE + INSERT approach instead of DROP + CREATE
                    df = obj[valid_columns]
                    
                    # Create a temporary table with the same structure
                    temp_table = f"temp_{table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    df.to_sql(
                        name=temp_table,
                        con=conn,
                        schema=schema,
                        index=False,
                        if_exists="replace"  # Safe for temp table
                    )
                    
                    # Clear existing data
                    cursor.execute(f"TRUNCATE TABLE {schema}.{table}")
                    
                    # Copy data from temp table to original table
                    cursor.execute(f"""
                        INSERT INTO {schema}.{table}
                        SELECT * FROM {schema}.{temp_table}
                    """)
                    
                    # Drop temp table
                    cursor.execute(f"DROP TABLE {schema}.{temp_table}")
                    raw_conn.commit()
                    
                else:
                    # If table doesn't exist yet, simple creation is fine
                    obj[valid_columns].to_sql(
                        name=table,
                        con=conn,
                        schema=schema,
                        if_exists="replace",
                        index=False,
                        chunksize=10000,
                        method="multi"
                    )