import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import time
import logging
from sqlalchemy import create_engine
import polars as pl

from postgres_utils import create_postgres_engine, copy_to_postgres, call_procedures, export_views_to_azure
from parquet_utils import process_parquet
from csv_utils import process_csv
from api_utils import process_json


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
        # "nppes_raw.parquet",
        # "nucc_taxonomy_250.csv",
        # "nppes_sample.csv"
        # "ssa_fips_state_county_2025.csv",
        # "ZIP_COUNTY_032025.xlsx",
        "https://api.census.gov/data/2023/acs/acs5?get=NAME,B01001_001E&for=county:*"
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
    table_names = []
    try:
        logging.info("Beginning batch processing...")
        for blob_name in files_to_process:           
            table_name = (
                str(f"{blob_name}")
                .replace("-", "_")
                .replace(".csv", "")
                .replace(".parquet", "")
                .replace(".xlsx", "")
            )
            table_names.append(table_name)

            if blob_name.endswith(".parquet"):
                summaries.append(process_parquet(blob_service_client, container_name, blob_name, postgres_engine, table_name))
            elif blob_name.endswith(".csv") or blob_name.endswith(".xlsx"):
                summaries.append(process_csv(blob_service_client, container_name, blob_name, postgres_engine, table_name))
            elif blob_name.startswith("https"):
                summaries.append(process_json(blob_name, postgres_engine, "gov_census_data"))

        if summaries:
            summary_df = pl.DataFrame(summaries)
            copy_to_postgres(summary_df, postgres_engine, "run_summary")
    except Exception as e:
        return func.HttpResponse(f"Error processing {blob_name}: {e}", status_code=400)

    # try:
    #     logging.info("Executing stored procedures...")
    #     call_procedures(postgres_engine, table_names)
    # except Exception as e:
    #     return func.HttpResponse(f"Error excuting stored procedures: {e}")

    # views_to_export = [
    #     "nppes_raw_summary",
    #     "nppes_sample_summary"
    # ]

    # try:
    #     logging.info("Exporting views to .csv...")
    #     for view in views_to_export:
    #         export_views_to_azure(blob_service_client, container_name, postgres_engine, view)
    # except Exception as e:
    #     return func.HttpResponse(f"Error exporting views to Azure Blob Storage: {e}")
    
    overall_end = time.perf_counter()
    logging.info(f"\nTotal Function Time: {overall_end - overall_start:.2f}s\n")

    return func.HttpResponse("Success", status_code=200)
