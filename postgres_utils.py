import os
import polars as pl
from sqlalchemy import inspect, text
import io

def create_postgres_engine():
    user = os.getenv("DATABASE_USER")
    password = os.getenv("DATABASE_PASSWORD")
    host = os.getenv("DATABASE_HOST")
    port = os.getenv("DATABASE_PORT")
    db = os.getenv("DATABASE_DB")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

def generate_ddl_from_polars(df: pl.DataFrame, table_name: str):
    type_mapping = {
        pl.Utf8: "TEXT",
        pl.Int64: "BIGINT",
        pl.Int32: "INTEGER",
        pl.Float64: "DOUBLE PRECISION",
        pl.Boolean: "BOOLEAN",
        pl.Datetime: "TIMESTAMP",
    }
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    for name, dtype in df.schema.items():
        sql_type = type_mapping.get(dtype, "TEXT")
        # Quote column names to handle spaces and special characters
        ddl += f'  "{name}" {sql_type},\n'
    ddl = ddl.rstrip(",\n") + "\n);"
    return ddl

def copy_to_postgres(polars_df, engine, table_name):
    insp = inspect(engine)
    db_tables = insp.get_table_names()
    if table_name not in db_tables:
        ddl = generate_ddl_from_polars(polars_df, table_name)
        with engine.connect() as conn:
            conn.execute(text(ddl))
            conn.commit()

    buffer = io.StringIO()
    polars_df.write_csv(buffer, include_header=False)
    buffer.seek(0)
    with engine.begin() as conn:
        raw_conn = conn.connection
        with raw_conn.cursor() as cur:
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)