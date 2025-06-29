import os
import pandas as pd
import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from io import StringIO

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def get_blob_service_client():
    conn_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(conn_string)


def download_file_from_blob(blob_service_client, container_name, blob_name):
    # Add an IF to be able to handle .xlsx and .csv (part3)
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        blob_bytes = blob_client.download_blob().readall()
        df = pd.read_csv(StringIO(blob_bytes.decode("utf-8")))
        print(f"Downloaded {blob_name} from container {container_name}")
        return df
    except Exception as e:
        print(f"Error downloading {blob_name} from Azure Blob Storage: {e}")


def push_to_postgres(dataframe, engine, blob_name):
    table_name = str(f"{blob_name}").replace(".csv", "")
    dataframe.head(0).to_sql(
        name=table_name, con=engine, if_exists="replace", index=False
    )
    dataframe.to_sql(name=table_name, con=engine, if_exists="append", index=False)
    print(f"Uploaded to Postgres as {table_name}")


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Triggered")

    blob_service_client = get_blob_service_client()
    container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

    files_to_process = ["nppes_sample.csv"]

    for blob_name in files_to_process:
        df = download_file_from_blob(blob_service_client, container_name, blob_name)
        print(df.head())
        push_to_postgres(
            dataframe=df, engine=os.getenv("POSTGRES_ENGINE"), blob_name=blob_name
        )

    return func.HttpResponse("Success", status_code=200)
