from datetime import datetime
import os
import zipfile
from celery import shared_task
from dotenv import load_dotenv

from loging.utils import delete_elasticsearch_documents

# Load environment variables
load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("LOGS_CONTAINER_NAME")
MAX_FILE_SIZE_LIMIT = 15000000000  # ~15GB

log_file_dir = "/logs/logs_web/"
log_file_name = "default.log"
log_file_path = os.path.join(log_file_dir, log_file_name)

# Initialize Azure Blob Client only if connection string exists
try:
    if AZURE_STORAGE_CONNECTION_STRING and CONTAINER_NAME:
        from azure.storage.blob import BlobServiceClient
        from azure.core.exceptions import AzureError, ResourceNotFoundError
        from utils.blob_functions import test_container_connection

        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    else:
        blob_service_client = None
        container_client = None
except Exception as e:
    print(f"[Azure Init Error] {e}")
    blob_service_client = None
    container_client = None


def get_most_recent_creation_date():
    try:
        blobs = list(container_client.list_blobs())
        if not blobs:
            return None
        most_recent_blob = max(blobs, key=lambda x: x["creation_time"])
        return most_recent_blob["creation_time"].date()
    except Exception as e:
        print(f"[Azure] Failed to get blob creation dates: {e}")
        return None


def zip_log_file(zip_file_name):
    try:
        zip_file_path_on_disk = os.path.join(log_file_dir, zip_file_name)
        with zipfile.ZipFile(zip_file_path_on_disk, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(log_file_path, os.path.basename(log_file_path))
        return zip_file_path_on_disk
    except Exception as e:
        print(f"[Zip Error] Failed to zip log file: {e}")
        return None


def rotate_logs():
    if not container_client or not blob_service_client:
        print("[RotateLogs] Azure connection not configured. Skipping upload.")
        return

    try:
        if not test_container_connection(AZURE_STORAGE_CONNECTION_STRING, CONTAINER_NAME):
            print("[Azure] Blob Storage test failed. Aborting rotation.")
            return

        end_date = get_most_recent_creation_date() or datetime.today().date()
        start_date = datetime.today().date()

        zip_file_name = f"{start_date.strftime('%d-%m-%Y')} - {end_date.strftime('%d-%m-%Y')}.zip"
        zip_path = zip_log_file(zip_file_name)
        if not zip_path:
            return

        blob_client = container_client.get_blob_client(zip_file_name)
        with open(zip_path, "rb") as file:
            blob_client.upload_blob(file, blob_type="BlockBlob")

        os.remove(zip_path)

        # Truncate local log file
        with open(log_file_path, "w") as log_file:
            log_file.truncate(0)

        print(f"[Log Rotate] Uploaded {zip_file_name} to Azure Blob and cleaned up locally.")
        delete_elasticsearch_documents()

    except Exception as e:
        print(f"[RotateLogs Error] {str(e)}")


@shared_task(name="check_size")
def check_file_size_limit():
    try:
        log_file_size = os.path.getsize(log_file_path)
        print(f"[Log Check] Current log file size: {log_file_size} bytes")
        if log_file_size >= MAX_FILE_SIZE_LIMIT:
            rotate_logs()
    except Exception as e:
        print(f"[Log Size Check Error] {e}")
