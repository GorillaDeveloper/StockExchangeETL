import requests
import os
import zipfile
from datetime import date, timedelta
import csv_fixer
import BucketToBigQuery
import gcp_bucket_apis

def download_file(url, folder_path):
    response = requests.get(url)

    text_file_name = current_date.strftime("%Y%d%b").lower()+".txt"
    text_file_path = os.path.join(folder_path, text_file_name)

    if response.status_code == 200:
        filename = url.split('/')[-1]
        save_path = os.path.join(folder_path, filename)
        if not os.path.exists(save_path):  # Check if the file already exists

            with open(save_path, 'wb') as file:
                file.write(response.content)

            print(f"File downloaded: {filename}")
            extract_zip(save_path, folder_path)  # Extract the ZIP file
            os.remove(save_path)  # Delete the ZIP file

            

            csv_fixer.remove_commas(text_file_path)

            gcp_bucket_apis.upload_to_bucket(text_file_name,text_file_path)

            BucketToBigQuery.upload_data_on_BQ(text_file_name)
        else:
            print(f"File already exists: {filename}")
            zip_filename = os.path.basename(save_path)
            extract_zip(save_path, folder_path)  # Extract the ZIP file
            os.remove(save_path)  # Delete the ZIP file

            # BucketToBigQuery.upload_data_on_BQ(text_file_path)
    else:
        print("Failed to download the file.")

def extract_zip(zip_path, extract_path):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            
        print(f"ZIP file extracted: {zip_path}")
    except:
        print(f"ZIP file extracted: {zip_path} is corrupted")
def create_folder_hierarchy(year, month):
    year_folder = str(year)
    month_folder = f"{month:02d}-{date(1900, month, 1).strftime('%b')}"
    folder_path = os.path.join(year_folder, month_folder)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path
def next_line():
    print('\n\r')
# Specify the start and end date range
start_date = date(2014, 2, 1)
end_date = date(2023, 5, 22)

# Iterate over the dates in the range
current_date = start_date
while current_date <= end_date:
    next_line()
    url = f"https://dps.psx.com.pk/download/symbol_price/{current_date.isoformat()}.zip"
    year = current_date.year
    month = current_date.month
    folder_path = create_folder_hierarchy(year, month)
    download_file(url, folder_path)

    current_date += timedelta(days=1)




