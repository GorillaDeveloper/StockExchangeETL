# step 1
 
import io
import os
 
import pandas as pd
import scripts.Logs as logs
# step 2
 
# path = 'data/SymbolsShortLongName'
# step 3
def ParseLIS(path,file_name,first_line_column,droped_column):
    # files = [f for f in os.listdir(path) if f.endswith('.lis')]
    # # step 4
    
    # for file in files:
    
    file_path = os.path.join(path, file_name)
    
    file_path = file_path.replace("\\", '/' )

    # step 5
    
    with open(file_path, 'r') as original:
        
        first_line = original.readline()
        if first_line != first_line_column:
            data = original.read()
            with open(file_path, 'w') as modified: modified.write(first_line_column+"\n" + data)
    # step 6
    
        df = pd.read_csv(file_path, delimiter="|")
    
        # df.columns = first_line_column.split('|')
        df = df.drop(df.columns[-3:], axis=1)
        # print(df)
        # df.columns = ['symbolic_name','short_name','long_name']
        
        first_line_column = first_line_column[:-3]
        # df.columns = ['Date','CompanySymbolName','CompanyCode','CompanyName','OpenRate','HighestRate','LowestRate','LastRateDiff','TurnOver','PreviousDayRate']
        df.columns = first_line_column.split('|')
        # print('lis file converted to dataframe')
        logs.print_message('lis file converted to dataframe')
        return df
        
        # print(df)
def Convert_LIS_To_CSV(df,path,file_name):

    file_path = os.path.join(path,file_name)
    csv_file_name = file_name.replace('.lis','.csv')
    csv_path = os.path.join(path,csv_file_name)

    df.to_csv(csv_path,index=False)
    # print('lis file converted to csv')
    logs.print_message('lis file converted to csv')

def ParseLIS_With_String_Content(content,first_line_column,drop_columns):
    # os.remove(os.path.join(base_path,base_file_name))
    df = pd.read_csv(io.StringIO(content), delimiter='|')
    df = df.drop(df.columns[-3:], axis=1)
    # print(df)
    # df.columns = ['symbolic_name','short_name','long_name']
    
    first_line_column = first_line_column[:-3]
    # df.columns = ['Date','CompanySymbolName','CompanyCode','CompanyName','OpenRate','HighestRate','LowestRate','LastRateDiff','TurnOver','PreviousDayRate']
    df.columns = first_line_column.split('|')
    # print('lis file converted to dataframe')
    logs.print_message('lis file converted to dataframe')
    return df

def DeleteLISFile(base_path,base_file_name):
    # print(os.path.join(base_path,base_file_name))
    logs.print_message(os.path.join(base_path,base_file_name))
    os.remove(os.path.join(base_path,base_file_name))
    # print(f'{base_file_name} file is deleted')
    logs.print_message(f'{base_file_name} file is deleted')