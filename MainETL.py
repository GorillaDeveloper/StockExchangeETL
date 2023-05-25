from datetime import date, timedelta
import os
import DownloadFilesFrom_PSX
import gcp_bucket_apis
import BucketToBigQuery
import csv_fixer



BUCKET_NAME = 'psx-data-bucket'
BUCKET_TEMP_LOCATION = f'gs://{BUCKET_NAME}/temp'

PSX_BASE_URL = 'https://dps.psx.com.pk/download/symbol_price/'

PROJECT_ID = 'youtubevideos-385412'
DATASET = 'PSXOpenAndClosingPricesDataset'
TABLE = 'OpenAndClosingPrices'
COLUMN_NAMES = ['MARKET_CODE','SYMBOL_CODE','SYMBOL_NAME','SETTLEMENT_TYPE','ORDER_REJECT_UPPER_PRICE','ORDER_REJECT_LOWER_PRICE','LAST_DAY_CLOSE_PRICE']
SCHEMA = 'MARKET_CODE:STRING,SYMBOL_CODE:STRING,SYMBOL_NAME:STRING,SETTLEMENT_TYPE:STRING,ORDER_REJECT_UPPER_PRICE:FLOAT,ORDER_REJECT_LOWER_PRICE:FLOAT,LAST_DAY_CLOSE_PRICE:FLOAT'

START_DATE = date(2014, 4, 15)
END_DATE =  date(2014, 12, 31)



def next_line():
    print('\n\r')
def join_folder_path(folder_path,file_name):
    newPath = os.path.join(folder_path,file_name).replace('\\','/')
    return newPath
    

def StartETL():

    next_line()
    CURRENT_DATE = START_DATE
    gcp_bucket_apis.create_bucket(BUCKET_NAME)
    BucketToBigQuery.create_dataset(DATASET)
    # folder_path = r''+DownloadFilesFrom_PSX.Create_Folder_Hierarchy(CURRENT_DATE)

    # print(folder_path)
    while CURRENT_DATE <=END_DATE:
    #     # year = current_date.year
    #     # month = current_date.month
    #     # folder_path = create_folder_hierarchy(year, month)

        folder_path =  DownloadFilesFrom_PSX.Create_Folder_Hierarchy(CURRENT_DATE)
        URL = PSX_BASE_URL+f'{CURRENT_DATE.isoformat()}.zip'
        DownloadFilesFrom_PSX.Download_PSX_Data_Files(URL,folder_path)


        text_file_name = CURRENT_DATE.strftime("%Y%d%b").lower()+".txt"
        text_file_path = join_folder_path(folder_path, text_file_name)
        csv_fixer.Remove_Commas(text_file_path)

        gcp_bucket_apis.Upload_To_Bucket(text_file_path,text_file_path)
        BucketToBigQuery.Upload_CSV_Data_In_BQ(text_file_path,BUCKET_NAME,BUCKET_TEMP_LOCATION,PROJECT_ID,DATASET,TABLE,COLUMN_NAMES,SCHEMA)
        
        next_line()
    #     next_line()
        CURRENT_DATE += timedelta(days=1)

StartETL()