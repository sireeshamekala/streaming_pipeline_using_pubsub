from google.cloud import storage
import json

storage_client = storage.Client()
DESTINATION_BUCKET = "destination_bucket_mayo"

def move_parquet_file(request):
    envelope = request.get_json()

    if not envelope:
        print("No event data")
        return "Bad Request", 400

    # Eventarc sends data inside "data"
    event = envelope.get("data", {})

    file_name = event.get("name")
    source_bucket_name = event.get("bucket")

    print(f"Event received for file: {file_name}")

    if not file_name:
        print("Missing file name")
        return "Bad Request", 400

    # Filter only parquet files
    if not file_name.endswith(".parquet"):
        print("Skipping non-parquet file.")
        return "OK", 200

    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(DESTINATION_BUCKET)

    blob = source_bucket.blob(file_name)

    # Copy
    source_bucket.copy_blob(blob, destination_bucket, file_name)
    print(f"Copied {file_name} to {DESTINATION_BUCKET}")

    # Delete
    blob.delete()
    print(f"Deleted {file_name} from {source_bucket_name}")

    return "Done", 200