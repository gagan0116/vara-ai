import os
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    if not bucket_name or not source_blob_name or not destination_file_name:
        print("Error: Missing GCS environment variables.")
        return

    # The client will use the environment credentials automatically
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # Ensure local directory exists
    directory = os.path.dirname(destination_file_name)
    if directory:
        os.makedirs(directory, exist_ok=True)

    print(f"Downloading {source_blob_name} from bucket {bucket_name} to {destination_file_name}...")
    try:
        blob.download_to_filename(destination_file_name)
        print("Download complete.")
    except Exception as e:
        print(f"Error during download: {e}")

if __name__ == "__main__":
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    BLOB_NAME = os.getenv("GCS_BLOB_NAME")
    DESTINATION = os.getenv("GCS_DESTINATION_PATH")

    download_blob(BUCKET_NAME, BLOB_NAME, DESTINATION)