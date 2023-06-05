import shutil
from google.cloud import storage
from google.cloud import bigquery


BUCKET_NAME = 'psx-upper-and-lower-price-symbol-data'
DATASET = 'PSXUperAndLowerPriceSymbolDataset'
FOLDER_PATH = './data/PSXOpeningAndClosingPrices/'
def delete_dataset(dataset_id):
    # Instantiate a BigQuery client
    client = bigquery.Client()

    # Construct the dataset reference
    dataset_ref = client.dataset(dataset_id)

    # Delete the dataset
    client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)

def delete_bucket(bucket_name):
    # Instantiate a client
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket(bucket_name)

    # Delete objects within the bucket
    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()

    # Delete the bucket
    bucket.delete(force=True)

def delete_folder(folder_path):
    # Delete the folder and its contents
    shutil.rmtree(folder_path)

delete_bucket(BUCKET_NAME)
delete_dataset(DATASET)
delete_folder(FOLDER_PATH)
