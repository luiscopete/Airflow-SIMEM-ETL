import subprocess, os

# path to the API_Request.sh file inside the scripts folder
current_directory = os.path.dirname(os.path.realpath(__file__))
scripts_directory = os.path.join(current_directory, '..' ,'scripts')
script_path = os.path.join(scripts_directory, 'API_Request.sh')

def api_get_file(dataset_id: str, startdate: str, enddate: str) -> None:
    ''' Run the API_Request.sh script to extract data from the API '''
    script_path = os.path.join(scripts_directory, 'API_Request.sh')
    print(script_path)
    subprocess.run(['chmod', '+x', script_path], check=True) #change permissions to execute the script
    subprocess.run([script_path, dataset_id, startdate, enddate], check=True) #shell=True)

def transform_json(dataset_id: str, output_file_name: str) -> None:
    ''' Run the transform_json.py script to transform the data '''
    file_path = os.path.join(current_directory, '..' ,'raw') # find folder in the parent directory
    output_path = os.path.join(current_directory, '..' , 'transformed') #folder in the parent directory
    script_path = os.path.join(scripts_directory, 'transform_json.sh')
    print(file_path)
    print(script_path)
    subprocess.run(['chmod', '+x', script_path], check=True) #change permissions to execute the script
    subprocess.run([script_path, file_path, dataset_id, output_path, output_file_name], check=True)

if __name__ == "__main__":
    #api_get_file(dataset_id='306c67', startdate='2046-12-01', enddate='2046-12-01')
    transform_json(dataset_id='306c67', output_file_name='test.txt')