import zipfile
import subprocess
import os
import platform
# this method is used to extract .Z files, it requieres winrar in the system, and installation path must be added in the environment vairablein PATH, and restart the visual code and the terminal/cmd
def Extract_Dot_Z_Files(file_path,output_directory,current_date):
    print(platform.system())
    try:
        if(platform.system() == 'Windows'):
            for_windows(file_path,output_directory,current_date)
        elif (platform.system() == 'Linux') :
            for_linux(file_path,output_directory,current_date)
        else:
            print('os does not support lis file extraction command')
    except Exception as ex:
        print(f'{file_path} file etracted: Failed : '+str(ex))

def for_windows(file_path,output_directory,current_date):
    try:
        temp_folder_name = os.path.join(output_directory, str(current_date.day)).replace('/','\\')
        temp_file_path = file_path.replace('/','\\')
        temp_output_directory = output_directory.replace('/','\\')
        temo_dot_Z_file_name = file_path.split('/')[-1:]
        
        os.mkdir(temp_folder_name)
        
        subprocess.run(f"move {temp_file_path} {temp_folder_name}\\;",shell=True )
        zip_file_path = os.path.join(temp_folder_name, temo_dot_Z_file_name[0])

        subprocess.run(["winrar", "x", zip_file_path, temp_folder_name], creationflags=subprocess.CREATE_NO_WINDOW)
        os.rename(f"{temp_folder_name}\\closing11.lis", f"{temp_folder_name}\\{current_date}.lis")
        
        subprocess.run(f"move {temp_folder_name}\\{current_date}.lis {temp_output_directory}\\;",shell=True )
        os.remove(f"{temp_folder_name}\\{temo_dot_Z_file_name[0]}")
        os.removedirs(temp_folder_name)
        print(f'{file_path} file etracted: Completed')
    except Exception as ex:
        print(f'{file_path} file etracted: Failed : '+str(ex))


def for_linux(file_path,output_directory,current_date):
    try:
        temp_folder_name = os.path.join(output_directory, str(current_date.day))
        temp_file_path = file_path
        temp_output_directory = output_directory
        temp_dot_Z_file_name = file_path.split('/')[-1:]

        print('i am trying to create folder at '+temp_folder_name+' and my current working directory is '+os.getcwd())
        print('output directory: '+output_directory)
        
        tt =  str(current_date)+'.lis'
        lis_file_name = file_path.replace('.Z','')
        print(f'lis file name: {lis_file_name}')
        print(f'mv {lis_file_name} {os.path.join(output_directory, tt)}')
        # os.mkdir(temp_folder_name)
        
        # subprocess.run(f"mv {temp_file_path} {temp_folder_name}/",shell=True )
        # zip_file_path = os.path.join(temp_folder_name, temo_dot_Z_file_name[0])
        subprocess.run(["gzip", "-d", ".Z", file_path])
        # subprocess.run(f'mv {lis_file_name} {os.path.join(output_directory, tt)}')
        
        # os.rename(f"{temp_folder_name}/{lis_file_name}", f"{temp_folder_name}/{current_date}.lis")
        
        # subprocess.run(f"mv {temp_folder_name}/{current_date}.lis {temp_output_directory}/",shell=True )
        # os.remove(f"{temp_folder_name}/{temo_dot_Z_file_name[0]}")
        # os.removedirs(temp_folder_name)
        print(f'{file_path} file etracted: Completed')
    except Exception as ex:
        print(f'{file_path} file etracted: Failed : '+str(ex))

def Extract_Zip_File(zip_path,extract_path):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            
        print(f"ZIP file extracted: {zip_path}")
    except:
        print(f"ZIP file extracted: {zip_path} is corrupted")

def Read_Lis_FileData_From_Dot_Z_File(file_path):
    output_file = 'closing11.lis'
    # Run the WinRAR command to extract the .Z file
    command = ['WinRAR', 'e', file_path, output_file]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _, error = process.communicate()

    if error:
        print(f"Extraction failed with error: {error.decode('utf-8')}")
        return None
    else:
        with open(output_file, 'r') as output:
            content = output.read()
            return content
            # print(content)


