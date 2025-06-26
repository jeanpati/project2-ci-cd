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

def download_file_from_blob(blob_service_client, container_name, blob_name, sheet_name):
    # Add an IF to be able to handle .xlsx and .csv (part3)
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_bytes = blob_client.download_blob().readall()  
        df = pd.read_csv(StringIO(blob_bytes), sheet_name=sheet_name)
        print(f"Downloaded {blob_name} from container {container_name}")            
        return df
    except Exception as e:
        print(f"Error downloading {blob_name} from Azure Blob Storage: {e}")   

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Triggered')




    return func.HttpResponse("Success", status_code=200)