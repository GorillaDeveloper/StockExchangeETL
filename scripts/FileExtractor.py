import zipfile
import subprocess
import os
import platform
import scripts.Logs as log
# this method is used to extract .Z files, it requieres winrar in the system, and installation path must be added in the environment vairablein PATH, and restart the visual code and the terminal/cmd
def Extract_Dot_Z_Files(file_path,output_directory,current_date):
    # print(platform.system())
    log.print_message("Operating System: "+platform.system())
    try:
        if(platform.system() == 'Windows'):
            for_windows(file_path,output_directory,current_date)
        elif (platform.system() == 'Linux') :
            for_linux(file_path,output_directory,current_date)
        else:
            # print('os does not support lis file extraction command')
            log.print_message('os does not support lis file extraction command')
    except Exception as ex:
        # print(f'{file_path} file etracted: Failed : '+str(ex))
        log.print_message(f'{file_path} file etracted: Failed : '+str(ex))

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
        # print(f'{file_path} file etracted: Completed')
        log.print_message(f'{file_path} file etracted: Completed')
    except Exception as ex:
        log.print_message(f'{file_path} file etracted: Failed : '+str(ex))
        # print(f'{file_path} file etracted: Failed : '+str(ex))

def for_linux(file_path,output_directory,current_date):
    try:
        log.print_message('file path: '+file_path)
        log.print_message('output directory path: '+output_directory)
        # print('file path: '+file_path)
        # print('output directory path: '+output_directory)
        lis_file_path = file_path.replace('.Z','')
        rename_file_path = os.path.join(output_directory,str(current_date)+'.lis')
        subprocess.run(["gzip", "-d", ".Z", file_path])
        subprocess.run(f"mv -f {lis_file_path} {rename_file_path}",shell=True )
        log.print_message(f'{file_path} file etracted: Completed')
        # print(f'{file_path} file etracted: Completed')
    except Exception as ex:
        log.print_message(f'{file_path} file etracted: Failed : '+str(ex))
        # print(f'{file_path} file etracted: Failed : '+str(ex))

def Extract_Zip_File(zip_path,extract_path):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            
        # print(f"ZIP file extracted: {zip_path}")
        log.print_message(f"ZIP file extracted: {zip_path}")
    except:
        print(f"ZIP file extracted: {zip_path} is corrupted")

def Read_Lis_FileData_From_Dot_Z_File(file_path):
    output_file = 'closing11.lis'
    # Run the WinRAR command to extract the .Z file
    command = ['WinRAR', 'e', file_path, output_file]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _, error = process.communicate()

    if error:
        # print(f"Extraction failed with error: {error.decode('utf-8')}")
        log.print_message(f"Extraction failed with error: {error.decode('utf-8')}")
        return None
    else:
        with open(output_file, 'r') as output:
            content = output.read()
            return content
            # print(content)


