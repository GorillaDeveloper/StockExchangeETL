import os
import pandas as pd

path = './2014/01-Jan/'

if os.path.exists(path):
    print(f"The directory '{path}' exists.")
else:
    print(f"The directory '{path}' does not exist.")

# print('current: ' + os.getcwd())

files = [f for f in os.listdir(path) if f.endswith('.txt')]

files_with_error = []

for file in files:
    try:
        file_path = os.path.join(path, file)
        file_path = file_path.replace("\\", '/')
        print('reading: ' + file_path)

        with open(file_path, 'r') as original:
            first_line = original.readline()

        df = pd.read_csv(file_path, delimiter=",")
        
        # Replace commas in a specific column
        target_column = 'SYMBOL_NAME'
        df[target_column] = df[target_column].str.replace(',', '')

        df = df.drop(df.columns[-1], axis=1)
        print(df)
    except Exception as exc:
        print(exc)
        # files_with_error.append(str(exc + " in " + file_path))

# for f in files_with_error:
#     print(f + ' has an error')
