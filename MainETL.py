import argparse
from datetime import date, timedelta,datetime
import glob
import multiprocessing
import os
import sys
import scripts.DownloadFilesFrom_PSX as downloadFile
import scripts.storage.gcp_bucket_apis as gcp_bucket
import scripts.bigquery.CustomBigQuery as bigquery
import scripts.parser.CSVParser as csvparser
import scripts.parser.LisParser as lisparser
import scripts.storage.mysqlcode.mysql_main as mysql
import scripts.FileExtractor as fileextractor
import scripts.Logs as log
import pandas as pd

BUCKET_NAME = 'psx-opening-and-closing-price-data'
BUCKET_TEMP_LOCATION = f'gs://{BUCKET_NAME}/temp'

PSX_OPENING_AND_CLOSING_PRICES_BASE_URL = 'https://dps.psx.com.pk/download/mkt_summary/'
PSX_SYMBOL_NANE_BASE_URL = 'https://dps.psx.com.pk/download/symbol_name/'

PROJECT_ID = 'youtubevideos-385412'
DATASET = 'PSXOpeningAndClosingPricesDataset2'
TABLE = 'OpeningAndClosingPrices'
COLUMN_NAMES = ['DATE_TIME','COMPANY_SYMBOL_NAME','COMPANY_CODE','COMPANY_NAME','OPEN_RATE','HIGHEST_RATE','LOWEST_RATE','LAST_RATE','TURN_OVER','PREVIOUS_DAY_RATE']
SCHEMA = 'DATE_TIME:STRING,COMPANY_SYMBOL_NAME:STRING,COMPANY_CODE:INTEGER,COMPANY_NAME:STRING,OPEN_RATE:FLOAT64,HIGHEST_RATE:FLOAT64,LOWEST_RATE:FLOAT64,LAST_RATE:FLOAT64,TURN_OVER:INTEGER,PREVIOUS_DAY_RATE:FLOAT64'

BASE_FOLDER_PATH_OF_PSX_OPENING_AND_CLOSING_PRICES_DATA=r'data/PSXOpeningAndClosingPrices/'

START_DATE = date(2013, 11, 4)
END_DATE =  date(2023, 7, 4)



    
def join_folder_path(folder_path,file_name):
    newPath = os.path.join(folder_path,file_name).replace('\\','/')
    return newPath

def Create_Folder_Hierarchy_PSXOpeningAndClosingPrices(current_date):

    year = current_date.year
    month = current_date.month

    year_folder = str(year)
    month_folder = f"{month:02d}-{date(1900, month, 1).strftime('%b')}"
    folder_path = join_folder_path(BASE_FOLDER_PATH_OF_PSX_OPENING_AND_CLOSING_PRICES_DATA+year_folder, month_folder)
    # os.makedirs(folder_path+'\\'+current_date.strftime('%d'), exist_ok=True)
    os.makedirs(folder_path, exist_ok=True)
    
    return folder_path


def DownloadFile_Process_1(dateStart,dateEnd,shared_memory):

    CurrentDate = dateStart
    while CurrentDate <=dateEnd:

        BaseFolderPath =  Create_Folder_Hierarchy_PSXOpeningAndClosingPrices(CurrentDate)
        DownloadedFileName= CurrentDate.strftime("%Y%m%d")+'_new.lis.Z'
        DownloadedFileNamePath = join_folder_path(BaseFolderPath,DownloadedFileName)

        ExtractedFileName = str(CurrentDate)+'.lis'
        ExtractedFileNamePath = join_folder_path(BaseFolderPath,ExtractedFileName)
        CSVFileName = CurrentDate.strftime("%Y%d%b").lower()+'.csv'

        url = PSX_OPENING_AND_CLOSING_PRICES_BASE_URL+f'{CurrentDate.isoformat()}.Z'
        downloadFile.Download_PSX_Data_Files(url,BaseFolderPath,DownloadedFileName)
        try:
            if(os.path.exists( DownloadedFileNamePath)):
                fileextractor.Extract_Dot_Z_Files(DownloadedFileNamePath,BaseFolderPath,CurrentDate)
                first_line = "DATE_TIME|COMPANY_SYMBOL_NAME|COMPANY_CODE|COMPANY_NAME|OPEN_RATE|HIGHEST_RATE|LOWEST_RATE|LAST_RATE|TURN_OVER|PREVIOUS_DAY_RATE|||"
                lis_df = lisparser.ParseLIS(BaseFolderPath,ExtractedFileName,first_line,3)
                csvparser.Convert_DataFrame_To_CSV(lis_df,BaseFolderPath,CSVFileName,False)
                os.remove(ExtractedFileNamePath)
                
                mysql.insert_dataframe(lis_df,'market_summary')
                #===================gcs api part====================================================
                # CSVFileNamePath = join_folder_path(BaseFolderPath, CSVFileName)
                # process1 = multiprocessing.Process(target=test,args=(CSVFileNamePath,CSVFileNamePath,shared_memory))
                # process1.start()
                #===================================================================================
                
            else:
                # print(f'{DownloadedFileName} not found')
                log.print_message(f'{DownloadedFileName} not found')
        except Exception as ex:
            # print(f'error thrown: {ex}')
            log.print_message(f'error thrown: {ex}')

        log.next_line()
        CurrentDate += timedelta(days=1)

def test(csv_file_blob,csv_file_path,shared_memory):
    # print('blob: '+csv_file_blob)
    
    log.print_message('blob: '+csv_file_blob)

    gcp_bucket.get_bucket(BUCKET_NAME)
    gcp_bucket.Upload_To_Bucket(csv_file_blob,csv_file_path)
    shared_memory.append(csv_file_blob)
    # CustomBigQuery.Transfer_CSV_From_GCS_To_BQ(csv_file_path,BUCKET_NAME,PROJECT_ID,DATASET,TABLE,BUCKET_TEMP_LOCATION,SCHEMA)
    
def Convert_Lis_File_To_CSV(base_folder,downloaded_file_name,extracted_file_name,csv_file_name,current_date):

    DownloadFileNamePath = join_folder_path(base_folder,downloaded_file_name)
    ExtractedFileNamePath = join_folder_path(base_folder,extracted_file_name)
    CSVFileNamePath = join_folder_path(base_folder,csv_file_name)

    if(os.path.exists( DownloadFileNamePath)):
            fileextractor.Extract_Dot_Z_Files(DownloadFileNamePath,base_folder,current_date)
            first_line = "DATE_TIME|COMPANY_SYMBOL_NAME|COMPANY_CODE|COMPANY_NAME|OPEN_RATE|HIGHEST_RATE|LOWEST_RATE|LAST_RATE|TURN_OVER|PREVIOUS_DAY_RATE|||"
            lis_df = lisparser.ParseLIS(base_folder,extracted_file_name,first_line,3)
            csvparser.Convert_DataFrame_To_CSV(lis_df,base_folder,csv_file_name,False)
            os.remove(ExtractedFileNamePath)
            CSVFileNamePath = join_folder_path(base_folder, csv_file_name)

            mysql.insert_dataframe_to_mysql(lis_df,'market_summary')

            # =====================GCS Bucket upload =========================
            # gcp_bucket_apis.Upload_To_Bucket(CSVFileNamePath,CSVFileNamePath)
            # process1 = multiprocessing.Process(target=test,args=(CSVFileNamePath,CSVFileNamePath))
            # process1.start()
            # ================================================================
            
    else:
        # print(f'{downloaded_file_name} not found')
        log.print_message(f'{downloaded_file_name} not found')

def StartETL():

    log.next_line()
    CURRENT_DATE = START_DATE
    gcp_bucket.create_bucket(BUCKET_NAME)
    bigquery.create_dataset(DATASET)

    while CURRENT_DATE <=END_DATE:

        BASE_FOLDER_PATH =  Create_Folder_Hierarchy_PSXOpeningAndClosingPrices(CURRENT_DATE)
        DOWNLOADED_FILE_NAME= CURRENT_DATE.strftime("%Y%m%d")+'_new.lis.Z'
        DOWNLOADED_FILE_NAME_PATH = join_folder_path(BASE_FOLDER_PATH,DOWNLOADED_FILE_NAME)

        EXTRACTED_FILE_NAME = 'closing11.lis'
        EXTRACTED_FILE_NAME_PATH = join_folder_path(BASE_FOLDER_PATH,EXTRACTED_FILE_NAME)
        # print(renamed_extracted_file_name_path)
        CSV_FILE_NAME = CURRENT_DATE.strftime("%Y%d%b").lower()+'.csv'

        URL = PSX_OPENING_AND_CLOSING_PRICES_BASE_URL+f'{CURRENT_DATE.isoformat()}.Z'
        downloadFile.Download_PSX_Data_Files(URL,BASE_FOLDER_PATH,DOWNLOADED_FILE_NAME)
        if(os.path.exists( DOWNLOADED_FILE_NAME_PATH)):
            fileextractor.Extract_Dot_Z_Files(DOWNLOADED_FILE_NAME_PATH,BASE_FOLDER_PATH)
            os.remove(DOWNLOADED_FILE_NAME_PATH)
            first_line = "DATE_TIME|COMPANY_SYMBOL_NAME|COMPANY_CODE|COMPANY_NAME|OPEN_RATE|HIGHEST_RATE|LOWEST_RATE|LAST_RATE|TURN_OVER|PREVIOUS_DAY_RATE|||"
            lis_df = lisparser.ParseLIS(BASE_FOLDER_PATH,EXTRACTED_FILE_NAME,first_line,3)
            csvparser.Convert_DataFrame_To_CSV(lis_df,BASE_FOLDER_PATH,CSV_FILE_NAME,False)
            os.remove(EXTRACTED_FILE_NAME_PATH)

            CSV_FILE_NAME_PATH = join_folder_path(BASE_FOLDER_PATH, CSV_FILE_NAME)

            # storage part starts here
            # ========================



            # merge_csv(CSV_FILE_NAME_PATH)
            # gcp_bucket_apis.Upload_To_Bucket(CSV_FILE_NAME_PATH,CSV_FILE_NAME_PATH)
            # CustomBigQuery.Upload_CSV_Data_In_BQ(CSV_FILE_NAME_PATH,BUCKET_NAME,BUCKET_TEMP_LOCATION,PROJECT_ID,DATASET,TABLE,COLUMN_NAMES,SCHEMA)
            # CustomBigQuery.Transfer_CSV_From_GCS_To_BQ(CSV_FILE_NAME_PATH,BUCKET_NAME,PROJECT_ID,DATASET,TABLE,BUCKET_TEMP_LOCATION,SCHEMA)
        else:
            # print(f'{DOWNLOADED_FILE_NAME} not found')
            log.print_message(f'{DOWNLOADED_FILE_NAME} not found')
        log.next_line()
        CURRENT_DATE += timedelta(days=1)

def MultiThreadedAndProcessing_ETL_Start():

    log.next_line()

    # gcp_bucket.create_bucket(BUCKET_NAME)
    # bigquery.create_dataset(DATASET)
    mysql.create_tables()
    mysql.connect()
    
    Duration = (END_DATE-START_DATE)
    remainder=Duration.days%2

    MidOfDuration = (Duration.days+remainder)/2


    manager = multiprocessing.Manager()
    shared_list = manager.list()

    process1 = multiprocessing.Process(target=DownloadFile_Process_1,args=(START_DATE,START_DATE+timedelta(days=MidOfDuration),shared_list))
    process2 = multiprocessing.Process(target=DownloadFile_Process_1,args=(START_DATE+timedelta(days=MidOfDuration+1),START_DATE+timedelta(days=Duration.days-remainder),shared_list))

    process1.start()
    process2.start()
    
    # process3 = multiprocessing.Process(target=CustomBigQuery.StartTransferingDataInLoopTo_BQ,args=(BUCKET_NAME
    #                                                                                                ,PROJECT_ID
    #                                                                                                ,DATASET
    #                                                                                                ,TABLE
    #                                                                                                ,BUCKET_TEMP_LOCATION
    #                                                                                                ,SCHEMA
    #                                                                                                ,shared_list
    #                                                                                                ,True))
    # process4 = multiprocessing.Process(target=CustomBigQuery.StartTransferingDataInLoopTo_BQ,args=(BUCKET_NAME
                                                                                                #    ,PROJECT_ID
                                                                                                #    ,DATASET
                                                                                                #    ,TABLE
                                                                                                #    ,BUCKET_TEMP_LOCATION
                                                                                                #    ,SCHEMA
                                                                                                #    ,shared_list
                                                                                                #    ,False))
    
    # process3.start()
    # process4.start()

    start = datetime.now()
    process1.join()
    process2.join()
    shared_list.append("end")
    shared_list.append("end")
    # process3.join()
    # process4.join()

    mysql.close_connection()

    log.next_line()
    
    log.print_message('Total Execution Time: '+str(datetime.now() - start))

    log.next_line()
    log.print_message('main ends')

def merge_csv():
    # Set the start and end dates for the desired date range
    # start_date = datetime(2013, 11, 1)
    # end_date = datetime(2023, 7, 30)

    # Initialize an empty DataFrame to store the combined data
    combined_data = pd.DataFrame()

    # Iterate over each date within the specified range
    current_date = START_DATE
    while current_date <= END_DATE:
        # Generate the folder path based on the current date
        folder_path = os.path.join(BASE_FOLDER_PATH_OF_PSX_OPENING_AND_CLOSING_PRICES_DATA, current_date.strftime("%Y/%d-%b"))

        # Check if the folder exists for the current date
        if os.path.exists(folder_path):
            print(f"{folder_path} exist")
            # Iterate over the files within the folder
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)

                # Check if the file is a CSV file
                if file_name.endswith('.csv'):
                    # Read the CSV file into a DataFrame
                    data = pd.read_csv(file_path)

                    # Append the data to the combined DataFrame
                    combined_data = pd.concat([combined_data, data], ignore_index=True)
                    print(f"{file_path} combined")


        # Increment the current date by 1 day
        current_date += timedelta(days=1)

    # Save the combined data to a new CSV file
    combined_data.to_csv(join_folder_path(BASE_FOLDER_PATH_OF_PSX_OPENING_AND_CLOSING_PRICES_DATA,"combined_file.csv"))
# merge_csv()
if __name__ == '__main__':
    if '--startdate' in sys.argv and '--enddate' in sys.argv:


        parser = argparse.ArgumentParser()
        parser.add_argument('--startdate', help='Start date value')
        parser.add_argument('--enddate', help='End date value')

        args = parser.parse_args()


        START_DATE = datetime.strptime(args.startdate, "%Y-%m-%d").date()
        # print(str(START_DATE))
        log.print_message("Start Date: "+str(START_DATE))
        END_DATE = datetime.strptime(args.enddate, "%Y-%m-%d").date()
        # print(str(END_DATE))
        log.print_message("End Date: "+str(END_DATE))
        MultiThreadedAndProcessing_ETL_Start()
    elif '--startdate' in sys.argv and not '--enddate' in sys.argv:

        parser = argparse.ArgumentParser()
        parser.add_argument('--startdate', help='Start date value')

        args = parser.parse_args()

        START_DATE = datetime.strptime(args.startdate, "%Y-%m-%d").date()
        
        log.print_message("Start Date: "+str(START_DATE))
        END_DATE = datetime.strptime(args.startdate, "%Y-%m-%d").date()
        # Logs.print_message("End Date: "+str(END_DATE))

        MultiThreadedAndProcessing_ETL_Start()
    elif '--gcstobq' in sys.argv:
        bigquery.Transfer_CSV_From_GCS_To_BQ()

    
