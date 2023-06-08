import os
import io
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import Logs

string_comma_repleaces_with =' '

def add_column(csv_data, new_column_name,value):
    try:
        df = pd.read_csv(io.StringIO(csv_data))
        df[new_column_name] = value
        columns_to_write = [col for col in df.columns if col != 'Unnamed: 0']
        return df[columns_to_write]
    except Exception as ex:
        Logs.print_message(f'ther is an error in adding column in csv_fixer {ex}')
        # print(f'ther is an error in adding column in csv_fixer {ex}')
def write_csv_data(df,output_file):
    df.to_csv(output_file, sep=',', index=False)
    
def Remove_In_String_Commas(txt_file):
    try:
        output_file = txt_file
        complete_text=""
        with open(txt_file, 'r') as file:
            # with open(output_file, 'w') as output:
            in_single_quotes = False
            inside_string = False
            current_string = ""
            for char in file.read():
                if char == "'":
                    if in_single_quotes:
                            in_single_quotes = False
                            inside_string = False
                            # output.write(char)
                            complete_text+=char
                            current_string = ""
                    else:
                        in_single_quotes = True
                        # output.write(char)
                        complete_text+=char
                elif char == ",":
                    if not inside_string:
                        # output.write(char)
                        complete_text+=char
                    else:
                        # output.write(string_comma_repleaces_with)
                        complete_text+=string_comma_repleaces_with
                else:
                    if in_single_quotes:
                        inside_string = True
                        current_string += char
                    # output.write(char)
                    complete_text+=char
            
            
        # print("Unnecessary commas removed and date column has been added successfully and saved to '" + txt_file + "'.\n\r")

        
        
        return complete_text
        # print(complete_text)
    # Usage example
        # with open(output_file, 'w') as file:
        #     file.write(complete_text)
    except Exception as ex:
        Logs.print_message (f'cannot remove additional commas in csv file due to this error: {ex}')
        # print (f'cannot remove additional commas in csv file due to this error: {ex}')

def join_folder_path(folder_path,file_name):
    newPath = os.path.join(folder_path,file_name).replace('\\','/')
    return newPath

def Convert_DataFrame_To_CSV(dataframe,file_path,file_name,indexing):
    
    dataframe.to_csv(join_folder_path(file_path,file_name),index=indexing)

    

