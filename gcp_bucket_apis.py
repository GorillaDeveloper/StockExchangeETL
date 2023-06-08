import os
from google.cloud import storage
from google.cloud.exceptions import Conflict
import Logs

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcs-private-key.json'
# bucket_name = 'psx-data-bucket'
storage_client = storage.Client()

files = []
global my_bucket 


"""
Upload Files in bucket
"""

def create_bucket(bucket_name):
    """
    Create n New Bucket
    """
    global my_bucket
    try:
        bucket = storage_client.bucket(bucket_name)
        bucket = storage_client.create_bucket(bucket,location ='us-east1')
        my_bucket = bucket
        Logs.print_message("bucket created")
    except Conflict:
        bucket = storage_client.get_bucket(bucket_name)
        my_bucket = bucket
        Logs.print_message("bucket already created")

    """
    Print Bucket Detail
    """
    vars(my_bucket)

    # """
    # Accessing aSpecific Bucket
    # """
    # my_bucket = storage_client.get_bucket(bucket_name)
def get_bucket(bucket_name):
    global my_bucket
    my_bucket = storage_client.get_bucket(bucket_name)
    return my_bucket
def Upload_To_Bucket(blob_name,file_path):
    if check_if_file_is_already_uploaded(blob_name) ==False:
        try:
            
            blob = my_bucket.blob(blob_name)
            blob.upload_from_filename(file_path)
            Logs.print_message(f'file name {blob_name} has been uploaded to gcs')
            return True
        except Exception as e:
            Logs.print_message(f'file name {blob_name} can not be uploaded due to these errors {e} to gcs')
            return False
    else:
        Logs.print_message (f'file {blob_name} is already uploaded to gcs')

"""
To check the current uploading file, either it is already available
"""
def check_if_file_is_already_uploaded(file_name):
    global my_bucket
    blob = my_bucket.blob(file_name)
    try:
        exists = blob.exists()
        return exists
    except FileNotFoundError:
        return False
