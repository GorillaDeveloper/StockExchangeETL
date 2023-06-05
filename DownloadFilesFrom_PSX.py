import requests
import os
import zipfile
from datetime import date, timedelta



def download_file(url, folder_path,file_name):

    response = requests.get(url)
    if response.status_code == 200:
        # filename = url.split('/')[-1]
        save_path = os.path.join(folder_path, file_name)
        if not os.path.exists(save_path):  # Check if the file already exists
            with open(save_path, 'wb') as file:
                file.write(response.content)
            print(f"File downloaded: {file_name}")
            # extract_zip(save_path, folder_path)  # Extract the ZIP file
            # os.remove(save_path)  # Delete the ZIP file
        else:
            print(f"File already exists: {file_name}")
            zip_filename = os.path.basename(save_path)
            # extract_zip(save_path, folder_path)  # Extract the ZIP file
            # os.remove(save_path)  # Delete the ZIP file

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





# Specify the start and end date range
start_date = date(2014, 2, 1)
end_date = date(2023, 5, 22)
# Iterate over the dates in the range
# current_date = start_date


def Download_PSX_Data_Files(url,folder_path,file_name):
    # while current_date <= end_date:
        # next_line()
        # url = f"https://dps.psx.com.pk/download/symbol_price/{current_date.isoformat()}.zip"
        # year = current_date.year
        # month = current_date.month
        # folder_path = create_folder_hierarchy(year, month)

        # download_file(url, folder_path)
        download_file(url,folder_path,file_name)

        # current_date += timedelta(days=1)


