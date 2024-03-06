from azure.identity import DefaultAzureCredential
from azure.identity import InteractiveBrowserCredential
from azure.storage.filedatalake import DataLakeServiceClient
import os

#credential = DefaultAzureCredential() #autenticación method
credential = InteractiveBrowserCredential() #autenticación method

def upload_file_to_data_lake(storage_account, destination_folder, process_folder, file_system, local_file_name, dl_file_name):
    ''' upload file to data lake
    Args: 
    storage_account: str: storage account name
    destination_folder: str: destination folder in the data lake
    process_folder: str: folder where the file is located
    file_system: str: file system name
    dl_file_name: str: name of the file to be uploaded'''
    account_url = f"https://{storage_account}.dfs.core.windows.net/"
    destination_path = f"/{destination_folder}/{dl_file_name}"  # Puedes especificar la ruta deseada
    service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    file_client = file_system_client.get_file_client(destination_path)
    #path to the raw folder in the my
    current_directory = os.path.dirname(os.path.realpath(__file__))
    process_directory = os.path.join(current_directory, '..' ,process_folder)
    local_file_path = os.path.join(process_directory, local_file_name)
    try:
        with open(local_file_path, "rb") as local_file:
            file_client.upload_data(local_file, overwrite=True)
        return True
    except Exception as e:
        raise e
