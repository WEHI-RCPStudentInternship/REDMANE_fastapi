import sqlite3
import csv
import argparse

# python import_rmh_patients.py 2 REDCAP-RMH-545455 redcap_rmh.csv  


# Set up command line argument parsing
parser = argparse.ArgumentParser(description='Import CSV data into SQLite database.')
parser.add_argument('project_id', type=int, help='The project ID to use for all records')
parser.add_argument('ext_patient_url', type=str, help='The place where this came from')
parser.add_argument('csv_file', type=str, help='The path to the CSV file to import')
args = parser.parse_args()

# Get the project_id and csv_file from the command line arguments
project_id = args.project_id
ext_patient_url = args.ext_patient_url
csv_file = args.csv_file

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('../data/data_redmane.db')
cur = conn.cursor()

# Open the CSV file and read its contents
with open(csv_file, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    
    # Insert data into the patients and patients_metadata tables
    for row in reader:
        # Insert into patients table
        cur.execute('''
        INSERT INTO patients (project_id, ext_patient_id,ext_patient_url)
        VALUES (?, ?, ?)
        ''', (project_id, row['record_id'], ext_patient_url))
        
        patient_id = cur.lastrowid
        
        # Insert into patients_metadata table
        cur.execute('''
        INSERT INTO patients_metadata (patient_id, key, value)
        VALUES (?, ?, ?)
        ''', (patient_id, 'age_range', row['age_range']))
        
        cur.execute('''
        INSERT INTO patients_metadata (patient_id, key, value)
        VALUES (?, ?, ?)
        ''', (patient_id, 'diabetes_1', row['diabetes_1']))
        
        cur.execute('''
        INSERT INTO patients_metadata (patient_id, key, value)
        VALUES (?, ?, ?)
        ''', (patient_id, 'diabetes_2', row['diabetes_2']))

# Commit the transaction and close the connection
conn.commit()
conn.close()

