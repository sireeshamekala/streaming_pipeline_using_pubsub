import csv
import time
import random
from faker import Faker
from google.cloud import storage
from datetime import datetime

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
BUCKET_NAME = "uploading_csv_to_gcs"          # <-- replace with your GCS bucket
OUTPUT_FOLDER = "dataflow_streaming_source"   # optional prefix inside GCS bucket
SLEEP_INTERVAL = 20                           # 1 minute

fake = Faker()

def generate_csv(filename):
    """Generate CSV with 10 records with integer IDs."""
    header = ["id", "name", "location"]

    rows = []
    for i in range(10):
        # Generate random integer between 1 and 100000 for id
        rows.append([
            random.randint(1, 100000),
            fake.name(),
            fake.city()
        ])

    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"[INFO] CSV generated: {filename}")
    return filename


def upload_to_gcs(local_file, bucket_name, dest_blob_name):
    """Upload file to GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)
    blob.upload_from_filename(local_file)

    print(f"[INFO] Uploaded to gs://{bucket_name}/{dest_blob_name}")


def main():
    while True:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"data_{timestamp}.csv"
        gcs_path = f"{OUTPUT_FOLDER}/{filename}"

        # Step 1: Generate CSV
        generate_csv(filename)

        # Step 2: Upload CSV to GCS
        upload_to_gcs(filename, BUCKET_NAME, gcs_path)

        print("[INFO] Waiting 1 minute...\n")
        time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    main()
