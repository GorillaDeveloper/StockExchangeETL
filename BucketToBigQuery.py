import apache_beam as beam
from google.cloud import bigquery
import csv
from google.cloud.exceptions import NotFound

# Set the GCS bucket, project ID, dataset, table, and schema
GCS_BUCKET = 'psx-data-bucket'
PROJECT_ID = 'youtubevideos-385412'
DATASET = 'PSXOpenAndClosingPricesDataset'
TABLE = 'OpenAndClosingPrices'
# SCHEMA = 'Sno:INTEGER,Title:STRING,Location:STRING,Date:STRING,Summary:STRING,Fatalities:INTEGER,Injured:INTEGER,TotalVictims:INTEGER,MentalHealthIssues:STRING,Race:STRING,Gender:STRING,Latitude:FLOAT,Longitude:FLOAT,IncidentArea:STRING,OpenCloseLocation:STRING,Target:STRING,Cause:STRING,PolicemanKilled:INTEGER,Age:STRING,Employeed_Y_N:STRING,EmployedAt:STRING'
SCHEMA = 'MARKET_CODE:STRING,SYMBOL_CODE:STRING,SYMBOL_NAME:STRING,SETTLEMENT_TYPE:STRING,ORDER_REJECT_UPPER_PRICE:FLOAT,ORDER_REJECT_LOWER_PRICE:FLOAT,LAST_DAY_CLOSE_PRICE:FLOAT'
TEMP_LOCATION = 'gs://psx-data-bucket/temp'

def parse_csv_line(line, column_names):
    reader = csv.reader([line.decode('Windows-1252')])
    try:
        columns = next(reader)
        return {column_names[i]: col for i, col in enumerate(columns)}
    except csv.Error:
        return None
def upload_data_on_BQ(file_name):
    client = bigquery.Client()

    dataset_ref = client.dataset(DATASET)

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{DATASET}' already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f"Dataset '{DATASET}' has been created.")

   

    with beam.Pipeline() as p:
        column_names = ['MARKET_CODE','SYMBOL_CODE','SYMBOL_NAME','SETTLEMENT_TYPE','ORDER_REJECT_UPPER_PRICE','ORDER_REJECT_LOWER_PRICE','LAST_DAY_CLOSE_PRICE']
        rows = p | 'Read CSV file' >> beam.io.ReadFromText(f'gs://{GCS_BUCKET}/{file_name}', skip_header_lines=2, coder=beam.coders.BytesCoder(), strip_trailing_newlines=True)
        parsed_rows = rows | 'Parse CSV line' >> beam.Map(parse_csv_line,column_names)
        filtered_rows = parsed_rows | 'Filter rows with missing values' >> beam.Filter(lambda row: row is not None)
        output = filtered_rows | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            f'{PROJECT_ID}:{DATASET}.{TABLE}',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            schema=SCHEMA,
            custom_gcs_temp_location=TEMP_LOCATION,
        )
    print(f'\n\r{file_name} data uploaded successfully')
