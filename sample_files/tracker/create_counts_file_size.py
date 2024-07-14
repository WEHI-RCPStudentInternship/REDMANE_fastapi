import os

def create_files(directory, file_size_mb, total_size_gb):
    """
    Create files in the specified directory until the total size reaches the desired amount.
    
    Args:
    directory (str): The directory to create the files in.
    file_size_mb (int): The size of each file in megabytes.
    total_size_gb (int): The total size of all files in gigabytes.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    total_size_bytes = total_size_gb * 1024**3
    file_size_bytes = file_size_mb * 1024**2
    num_files = total_size_bytes // file_size_bytes
    
    for i in range(num_files):
        file_path = os.path.join(directory, f"file_{i}.test.dat")
        with open(file_path, "wb") as f:
            f.write(os.urandom(file_size_bytes))
        print(f"Created {file_path}")

# Parameters
directory_to_create_files = "scrnaseq/raw"  # Replace with your desired directory
file_size_mb = 10  # Size of each file in MB
total_size_gb = 3  # Total size of all files in GB

# Create the files
create_files(directory_to_create_files, file_size_mb, total_size_gb)

