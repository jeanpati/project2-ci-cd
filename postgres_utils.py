import os
import polars as pl
from sqlalchemy import inspect, text
import io
import csv
import logging
import gc

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

def call_procedures(engine, table_names):
    cleaning_procedures = []
    for table_name in table_names:
        cleaning_procedures.append(f"CALL rename_columns_with_special_chars('{table_name}');")
    try:
        for procedure in cleaning_procedures:
            logging.info(f"Executing: '{procedure}'...")
            with engine.begin() as conn:
                conn.execution_options(autocommit=True).execute(text(procedure))
    except Exception as e:
        print(f"Error executing '{procedure}': {e}")
        return

    summary_view_procedures = [
        "CALL create_nppes_csv('nppes_sample');",
        "CALL create_nppes_csv('nppes_raw');",
    ]

    try:
        for procedure in summary_view_procedures:
            logging.info(f"Executing: '{procedure}'...")
            with engine.begin() as conn:
                conn.execute(text(procedure))
    except Exception as e:
        print(f"Error executing procedure '{procedure}' : {e}")
        return

    complete_view_procedures = [
        "CALL create_nppes_table_with_county();",
    ]

    try:
        for procedure in complete_view_procedures:
            logging.info(f"Executing: '{procedure}'...")
            with engine.begin() as conn:
                conn.execute(text(procedure))
    except Exception as e:
        print(f"Error executing procedure '{procedure}' : {e}")
        return

def export_views_to_azure(blob_service_client, container_name, engine, views):
        for view in views:
            try:
                blob_client = blob_service_client.get_blob_client(
                    container=container_name, blob=f"{view}.csv"
                )
                with engine.begin() as conn:
                    records = conn.execute(text(f"SELECT * FROM {view}"))
                    rows = records.fetchall()
                    columns = list(records.keys())

                    csv_buffer = io.StringIO()
                    writer = csv.writer(csv_buffer)
                    writer.writerow(columns)
                    writer.writerows(rows)
                    csv_data = csv_buffer.getvalue()

                blob_client.upload_blob(csv_data, overwrite=True)
                logging.info(f"Exported {view}.csv to Azure Blob Storage!")

                del rows, columns, csv_data, csv_buffer, writer, records
                gc.collect()

            except Exception as e:
                print(f"Error exporting '{view}' to .csv: {e}")