import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import time
import logging
from sqlalchemy import create_engine
import polars as pl

from postgres_utils import create_postgres_engine, copy_to_postgres
from parquet_utils import process_parquet
from csv_utils import process_csv


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def get_blob_service_client():
    conn_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(conn_string)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("azure.storage").setLevel(logging.WARNING)

    overall_start = time.perf_counter()

    files_to_process = [
        "nucc_taxonomy_250.csv",
        "nppes_sample.csv",
        "ssa_fips_state_county_2025.csv"
    ]

    try:
        logging.info("Creating Postgres engine...")
        postgres_engine = create_engine(create_postgres_engine(), echo=False)
    except Exception as e:
        return func.HttpResponse(f"Error fetching connection to Postgres engine: {e}", status_code=400)

    try:
        logging.info("Connecting to Azure Blob Service Client...")
        blob_service_client = get_blob_service_client()
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    except Exception as e:
        return func.HttpResponse(f"Error fetching connection to Azure Blob Service Client: {e}", status_code=400)

    summaries = []
    try:
        for blob_name in files_to_process:           
            table_name = (
                str(f"{blob_name}")
                .replace("-", "_")
                .replace(".csv", "")
                .replace(".parquet", "")
            )
            if blob_name.endswith(".parquet"):
                summaries.append(process_parquet(blob_service_client, container_name, blob_name, postgres_engine, table_name))
            elif blob_name.endswith(".csv"):
                summaries.append(process_csv(blob_service_client, container_name, blob_name, postgres_engine, table_name))

        if summaries:
            summary_df = pl.DataFrame(summaries)
            copy_to_postgres(summary_df, postgres_engine, "run_summary")
    except Exception as e:
        return func.HttpResponse(f"Error processing {blob_name}: {e}", status_code=400)
    
    overall_end = time.perf_counter()
    logging.info(f"\nTotal Function Time: {overall_end - overall_start:.2f}s")

    return func.HttpResponse("Success", status_code=200)
