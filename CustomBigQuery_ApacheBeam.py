import apache_beam as beam
from google.cloud import bigquery
import csv
from google.cloud.exceptions import NotFound

def parse_csv_line(line, column_names):
    reader = csv.reader([line.decode('Windows-1252')])
    try:
        columns = next(reader)
        return {column_names[i]: col for i, col in enumerate(columns)}
    except csv.Error as ex:
        print('parsing error: '+ex)
        return None
    
def create_dataset(dataset_name):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_name}' already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f"Dataset '{dataset_name}' has been created.")

def Upload_CSV_Data_In_BQ(file_name,gcs_bucket,gcs_bucket_temp,project_id,data_set,table,csv_column_names,csv_schema):
    
    try:
       
        with beam.Pipeline() as p:
            
            column_names =csv_column_names
            rows = p | 'Read CSV file' >> beam.io.ReadFromText(f'gs://{gcs_bucket}/{file_name}', skip_header_lines=1, coder=beam.coders.BytesCoder(), strip_trailing_newlines=True)
            parsed_rows = rows | 'Parse CSV line' >> beam.Map(parse_csv_line,column_names)
            filtered_rows = parsed_rows | 'Filter rows with missing values' >> beam.Filter(lambda row: row is not None)
            output = filtered_rows | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                f'{project_id}:{data_set}.{table}',
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                schema=csv_schema,
                custom_gcs_temp_location=gcs_bucket_temp,
            )
        
        print(f'\n\r{file_name} data uploaded successfully to big query')
    except Exception as ex:
        print(f'{file_name} file data can not be transfered to big query {ex}')

