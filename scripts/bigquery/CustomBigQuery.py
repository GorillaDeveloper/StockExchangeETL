import os
import scripts.Logs as log
import time
from google.cloud import bigquery
from google.cloud import storage
from datetime import date, timedelta,datetime

def Transfer_CSV_From_GCS_To_BQ(file_name,bucket_name,  project_id, dataset_id, table_id,temp_location,new_schema):
    # Initialize the BigQuery client
    try:
        bq_client = bigquery.Client(project=project_id)

        # Initialize the Storage client
        storage_client = storage.Client(project=project_id)

        # Get the GCS bucket
        bucket = storage_client.get_bucket(bucket_name)

        # Define the GCS file path
        file_path = f"gs://{bucket_name}/{file_name}"

        # Define the BigQuery table reference
        table_ref = bq_client.dataset(dataset_id).table(table_id)

        # Define the job configuration
        job_config = bigquery.LoadJobConfig(
            # schema=new_schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True # Set to False if you have a predefined schema
        )

        # Start the BigQuery load job
        load_job = bq_client.load_table_from_uri(
            file_path,
            table_ref,
            job_config=job_config
        )

        # Wait for the load job to complete
        load_job.result()

        # Check if the load job was successful
        if load_job.state == "DONE":
            log.print_message(f"CSV file {file_name} transferred to BigQuery table {table_id} successfully.")
            # print(f"CSV file {file_name} transferred to BigQuery table {table_id} successfully.")
        else:
            # print(f"Failed to transfer CSV file {file_name} to BigQuery table {table_id}.")
            log.print_message(f"Failed to transfer CSV file {file_name} to BigQuery table {table_id}.")
    except Exception as ex:
        log.print_message(f'Brig Query failed to transfer this file due to this error: {ex}')

def create_dataset(dataset_name):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        # print(f"Dataset '{dataset_name}' already exists.")
        log.print_message(f"Dataset '{dataset_name}' already exists.")
    except Exception as ex:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        # print(f"Dataset '{dataset_name}' has been created.")
        log.print_message(f"Dataset '{dataset_name}' has been created.")


def StartTransferingDataInLoopTo_BQ(bucket_name,  project_id, dataset_id, table_id,temp_location,new_schema,shared_memory,isWorkingOnOddIndexs):
    firstIndex = 0
    transactionalPauseTime = 0.5
    if isWorkingOnOddIndexs:
        firstIndex=1
        # transactionalPauseTime = 1
    

    try:
        while True:
            temp_list  = list(shared_memory)
            if(firstIndex < len(shared_memory)):
                if(os.path.exists(temp_list[firstIndex]) ):
                    tempfilename = temp_list[firstIndex]
                    shared_memory.remove(tempfilename)
                    Transfer_CSV_From_GCS_To_BQ(tempfilename,bucket_name,  project_id, dataset_id, table_id,temp_location,new_schema)
                    # print("in the custom big query: "+tempfilename)
                    log.print_message("in the custom big query: "+tempfilename)
                    if(temp_list[firstIndex] == 'end'):
                        break
                    # firstIndex += 2
                    time.sleep(transactionalPauseTime)
                else:
                    # print('file path does not exist, so i am waiting...'+str(firstIndex))
                    log.print_message('file path does not exist, so i am waiting...'+str(firstIndex))
                    if(temp_list[firstIndex] == 'end'):
                        break
                    time.sleep(transactionalPauseTime)
            else:
                # print('no file paths exist in shared memory, i am waiting... index" '+str(firstIndex))
                log.print_message('no file paths exist in shared memory, i am waiting... index" '+str(firstIndex))
                # if(temp_list[firstIndex-1] == 'end'):
                #     break
                time.sleep(transactionalPauseTime)
    except Exception as ex:
        log.print_message(f'Brig Query multi process stops due to this error: {ex}')

    
        

        