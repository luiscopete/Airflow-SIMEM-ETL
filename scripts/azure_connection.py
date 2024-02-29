from azure.identity import DefaultAzureCredential
from azure.identity import InteractiveBrowserCredential
from azure.storage.filedatalake import DataLakeServiceClient

credential = InteractiveBrowserCredential()

account_url = "https://stgpersonalprj.dfs.core.windows.net/"

file_system_name = "raw"
file_name = "requirements.txt"

destination_path = "/folder/{}" .format(file_name)  # Puedes especificar la ruta deseada

local_file_path = "./requirements.txt"

service_client = DataLakeServiceClient(account_url=account_url, credential=credential)

file_system_client = service_client.get_file_system_client(file_system=file_system_name)

file_client = file_system_client.get_file_client(destination_path)

with open(local_file_path, "rb") as local_file:
    file_client.upload_data(local_file, overwrite=True)

