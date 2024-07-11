import os,re
import requests
import argparse

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


print(sample_data)
print(dataset_metadata)
print("=============================")

# Find files
found_files = find_files(directory_to_search, extension)

if sample_info_stored == "header":
    # Print the found files
    for file in found_files:
        print("=============================")
        status = "Not found"

        print(file)
        print(status)


if sample_info_stored == "filename":
    # Print the found files
    for file in found_files:
        print("=============================")
        status = "Not found"
        for data in sample_data:
            if data["ext_sample_id"] in file:
                status = "sample_found"
            elif check_patient_in_filename(file, data["ext_patient_id"]):
                status = "patient_found"

        print(file)
        print(status)
