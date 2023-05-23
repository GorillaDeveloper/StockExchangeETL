import os
from google.cloud import storage
from google.cloud.exceptions import Conflict

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_Google.json'
bucket_name = 'psx-data-bucket'
storage_client = storage.Client()

files = []
# bucket = ''



"""
Upload Files in bucket
"""

def create_bucket():
    """
    Create n New Bucket
    """

    try:
        bucket = storage_client.bucket(bucket_name)
        bucket = storage_client.create_bucket(bucket,location ='us-east1')
    except Conflict:
        print("bucket already created")

    """
    Print Bucket Detail
    """
    vars(bucket)

    # """
    # Accessing aSpecific Bucket
    # """
    # my_bucket = storage_client.get_bucket(bucket_name)

def upload_to_bucket(blob_name,file_path):
    create_bucket()
    bucket = storage_client.get_bucket(bucket_name)
    if check_if_file_is_already_uploaded(blob_name,bucket) ==False:
        try:
            
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file_path)
            print(f'file name {blob_name} has been uploaded')
            return True
        except Exception as e:
            print(f'file name {blob_name} can not be uploaded due to these errors {e}')
            return False
    else:
        print (f'file {blob_name} is already uploaded')

"""
To check the current uploading file, either it is already available
"""
def check_if_file_is_already_uploaded(file_name,bucket):
    blob = bucket.blob(file_name)
    try:
        exists = blob.exists()
        return exists
    except FileNotFoundError:
        return False
