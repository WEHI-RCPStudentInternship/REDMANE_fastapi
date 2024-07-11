import os,re
import requests

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


# Example usage
directory_to_search = "."  # Replace with your directory path
raw_file_extensions = "*.fastq"  # Example extension
sample_info_stored = "filename"  # Example extension
extension = raw_file_extensions.lstrip("*")  # Remove the asterisk to get the actual extension

# Find files
found_files = find_files(directory_to_search, extension)

# Get sample data
url = "http://localhost:8888/samples/0?project_id=1"
sample_data = get_sample_data(url)


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
