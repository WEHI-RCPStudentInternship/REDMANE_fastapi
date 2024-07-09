import sqlite3
import csv
import argparse

# Function to create SQLite tables if they do not exist
def create_tables(cur):
    cur.execute('''
    CREATE TABLE IF NOT EXISTS patients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id TEXT NOT NULL,
        ext_patient_id TEXT,
        ext_patient_url TEXT,
        public_patient_id TEXT
    );
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS samples (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        patient_id INTEGER NOT NULL,
        ext_sample_id TEXT,
        ext_sample_url TEXT,
        FOREIGN KEY (patient_id) REFERENCES patients(id)
    );
    ''')
    
    cur.execute('''
    CREATE TABLE IF NOT EXISTS samples_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sample_id INTEGER NOT NULL,
        key TEXT,
        value TEXT,
        FOREIGN KEY (sample_id) REFERENCES samples(id)
    );
    ''')

# Function to import data from CSV into SQLite tables
def import_csv_to_sqlite(conn, project_id, ext_sample_url, csv_file):
    cur = conn.cursor()

    # Create tables if they do not exist
    create_tables(cur)

    # Open the CSV file and read its contents
    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Process each row in the CSV file
        for row in reader:
            # Fetch patient_id based on project_id and record_id (ext_patient_id)
            cur.execute('''
            SELECT id FROM patients WHERE project_id = ? AND ext_patient_id = ?
            ''', (project_id, row['record_id']))
            patient_id = cur.fetchone()
            if patient_id:
                patient_id = patient_id[0]
            else:
                print(f"No patient found with project_id '{project_id}' and record_id '{row['record_id']}'. Skipping row.")
                continue

            # Insert into samples table
            cur.execute('''
            INSERT INTO samples (patient_id, ext_sample_id, ext_sample_url)
            VALUES (?, ?, ?)
            ''', (patient_id, row['sample_id'], ext_sample_url))
            
            sample_id = cur.lastrowid
            
            # Insert into samples_metadata table
            cur.execute('''
            INSERT INTO samples_metadata (sample_id, key, value)
            VALUES (?, ?, ?)
            ''', (sample_id, 'ext_sample_batch', row['ext_sample_batch']))

            cur.execute('''
            INSERT INTO samples_metadata (sample_id, key, value)
            VALUES (?, ?, ?)
            ''', (sample_id, 'tissue', row['tissue']))
            
            cur.execute('''
            INSERT INTO samples_metadata (sample_id, key, value)
            VALUES (?, ?, ?)
            ''', (sample_id, 'sample_date', row['sample_date']))

    # Commit the transaction
    conn.commit()

# Main execution when running the script
if __name__ == "__main__":
    # Command line argument parsing
    parser = argparse.ArgumentParser(description='Import CSV data into SQLite database.')
    parser.add_argument('project_id', type=str, help='The project ID to use to find the patients')
    parser.add_argument('ext_sample_url', type=str, help='The external sample URL to populate in samples')
    parser.add_argument('csv_file', type=str, help='The path to the CSV file to import')
    args = parser.parse_args()

    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('../data/data_redmane.db')

    # Call function to import data into SQLite tables
    import_csv_to_sqlite(conn, args.project_id, args.ext_sample_url, args.csv_file)

    # Close the database connection
    conn.close()

