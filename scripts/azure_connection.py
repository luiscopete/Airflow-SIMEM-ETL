from azure.identity import DefaultAzureCredential
from azure.identity import InteractiveBrowserCredential
from azure.storage.filedatalake import DataLakeServiceClient

#credential = DefaultAzureCredential() #autenticación method
credential = InteractiveBrowserCredential() #autenticación method

def upload_file_to_data_lake(storage_account, destination_folder, local_file_path, file_system, dl_file_name):
    ''' upload file to data lake
    Args: 
        storage_account (str): storage account name
        destination_path (str): destination path in the data lake
        local_file_path (str): local file path to upload 
        dl_file_name (str): file name in the data lake'''
    account_url = f"https://{storage_account}.dfs.core.windows.net/"
    destination_path = f"/{destination_folder}/{dl_file_name}"  # Puedes especificar la ruta deseada
    service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    file_client = file_system_client.get_file_client(destination_path)
    try:
        with open(local_file_path, "rb") as local_file:
            file_client.upload_data(local_file, overwrite=True)
        return True
    except Exception as e:
        raise e
