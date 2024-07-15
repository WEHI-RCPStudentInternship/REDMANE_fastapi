import requests
from datetime import datetime
import os,re
import requests
import argparse
import subprocess
import platform
import json

def get_total_size(extension,directory):
    total_size = 0
    for root, dirs, files in os.walk('.'):
        for file in files:
            if file.endswith(extension):
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
    return total_size

def find_files(directory, extension=".fastq"):
    """
    Recursively search for files with the given extension in the specified directory.
    
    Args:
    directory (str): The root directory to start the search from.
    extension (str): The file extension to search for (default is ".fastq").
    
    Returns:
    list: A list of paths to files matching the extension.
    """
    matches = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(extension):
                matches.append(os.path.join(root, file))
    return matches

def get_dataset_metadata(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad status codes
    dataset = response.json()

    result = {}
    for metadata in dataset["metadata"]:
        if metadata["key"] == "sample_info_stored":
            result["sample_info_stored"] = metadata["value"]
        if metadata["key"] == "raw_file_extensions":
            result["raw_file_extensions"] = metadata["value"]

    return result

def get_sample_data(url):
    """
    Fetch sample data from the given URL and extract ext_sample_id and patient_id.
    
    Args:
    url (str): The URL to fetch the sample data from.
    
    Returns:
    list: A list of dictionaries containing ext_sample_id and patient_id.
    """
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad status codes
    samples = response.json()

    result = []
    for sample in samples:
        result.append({
            "sample_id": sample["id"],
            "patient_id": sample["patient_id"],
            "ext_sample_id": sample["ext_sample_id"],
            "ext_patient_id": sample["patient"]["ext_patient_id"]
        })
    
    return result

def check_patient_in_filename(filename, ext_patient_id):
    """
    Check if ext_patient_id is found in the filename, treating spaces as wildcards.
    
    Args:
    filename (str): The filename.
    ext_patient_id (str): The ext_patient_id to search for, with spaces treated as wildcards.
    
    Returns:
    bool: True if the ext_patient_id is found in the filename, False otherwise.
    """
    # Create a regex pattern to match ext_patient_id with spaces as wildcards
    pattern = re.compile(re.escape(ext_patient_id).replace(r'\ ', '.*'))
    
    if pattern.search(filename):
        return True
    
    return False

parser = argparse.ArgumentParser(description='Search for patient or sample IDs in file names.')
parser.add_argument('--directory', type=str, help='The root directory to search')
parser.add_argument('--dataset_id', type=int, required=True, help='The dataset ID to use')
parser.add_argument('--project_id', type=int, required=True, help='The project ID to use')
args = parser.parse_args()

directory_to_search = args.directory
dataset_id = args.dataset_id
project_id = args.project_id

print(args)


# Get sample data
url = "http://localhost:8888/samples/0?project_id="+str(project_id)
sample_data = get_sample_data(url)

url = "http://localhost:8888/datasets_with_metadata/"+str(dataset_id)+"?project_id="+str(project_id)
dataset_metadata = get_dataset_metadata(url)

sample_info_stored = dataset_metadata["sample_info_stored"]
raw_file_extensions = dataset_metadata["raw_file_extensions"]
extension = raw_file_extensions.lstrip("*")  # Remove the asterisk to get the actual extension

total_size_bytes = get_total_size(extension,directory_to_search)
total_size_mb = total_size_bytes / (1024 * 1024)
print(f"Total size of files with extension '{extension}': {total_size_mb:.2f} MB")


# Run the command and capture the output
# Check the operating system
os_type = platform.system()

if os_type == 'Linux':
    command = ['du', '--max-depth=1', '-m', directory_to_search]
elif os_type == 'Darwin':  # macOS
    command = ['du', '-d', '1', '-m', directory_to_search]
else:
    raise OSError(f"Unsupported operating system: {os_type}")

result = subprocess.run(command, capture_output=True, text=True)

# Get today's date
today_date = datetime.now().strftime('%Y-%m-%d')

update_dataset_metadata_size = {
        "dataset_id": dataset_id,
        "raw_file_size": str(int(total_size_mb))+"MB" ,
        "last_size_update": today_date
    }


# Find files
found_files = find_files(directory_to_search, extension)

update_raw_files = [] 

if sample_info_stored == "header":
    # Print the found files
    for file in found_files:
        try:
            with open(file, 'r') as file:
                header = file.readline().strip()
        except Exception as e:
            print(f"Error reading file {filename}: {e}")

        components = re.split(r'\s+', header.strip())
        for data in sample_data:
            if data["ext_sample_id"] in components:
                status = "sample_found"
                update_raw_files.append({"raw_file": file.name,"sample_id":data["sample_id"],"dataset_id":dataset_id,"project_id":project_id})
                

if sample_info_stored == "filename":
    # Print the found files
    for file in found_files:
        status = "Not found"
        for data in sample_data:
            if data["ext_sample_id"] in file:
                update_raw_files.append({"path": file,"dataset_id":dataset_id,"metadata":[{"metadata_key": "sample_id","metadata_value":str(data["sample_id"])}]})
            elif check_patient_in_filename(file, data["ext_patient_id"]):
                status = "patient_found"
                update_raw_files.append({"path": file,"dataset_id":dataset_id,"metadata":[{"metadata_key": "sample_id","metadata_value":str(data["sample_id"])}]})
                break

print(json.dumps(update_raw_files,indent=2))
print(json.dumps(update_dataset_metadata_size,indent=2))

# Send PUT request to update dataset metadata
update_metadata_url = 'http://localhost:8888/datasets_metadata/size_update'
headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}
response = requests.put(update_metadata_url, headers=headers, json=update_dataset_metadata_size)
response.raise_for_status()
print(f"Metadata update response: {response.status_code} {response.reason}")



# Send POST request to add raw files
add_raw_files_url = 'http://localhost:8888/add_raw_files/'
response = requests.post(add_raw_files_url, headers=headers, json=update_raw_files)
response.raise_for_status()
print(f"Raw files update response: {response.status_code} {response.reason}")
