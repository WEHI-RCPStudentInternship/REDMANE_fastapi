from fastapi import FastAPI
import sqlite3
from pydantic import BaseModel
from typing import List, Optional


# Create an instance of FastAPI
app = FastAPI()


DATABASE = 'data/data_redmane.db'

# Initialize the database and create the tables if they don't exist
def init_db():
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        status TEXT
    );
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS datasets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        name TEXT,
        FOREIGN KEY (project_id) REFERENCES projects(id)
    );
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS patients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        ext_patient_id TEXT,
        ext_patient_url TEXT,
        public_patient_id TEXT,
        FOREIGN KEY (project_id) REFERENCES projects(id)
    );
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS patients_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        patient_id INTEGER NOT NULL,
        key TEXT,
        value TEXT,
        FOREIGN KEY (patient_id) REFERENCES patients(id)
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

    cur.execute('''
    CREATE TABLE IF NOT EXISTS raw_file_information (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dataset_id INTEGER NOT NULL,
        directory TEXT,
        FOREIGN KEY (dataset_id) REFERENCES datasets(id)
    );
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS raw_files_metadata (
	metadata_id INTEGER PRIMARY KEY AUTOINCREMENT,
	raw_file_id INTEGER,
	metadata_key TEXT NOT NULL,
	metadata_value TEXT NOT NULL,
	FOREIGN KEY (raw_file_id) REFERENCES raw_files (id)
    );
    ''')

    conn.commit()
    conn.close()

# Call the function to initialize the database
init_db()

# Pydantic model for Project
class Project(BaseModel):
    id: int
    name: str
    status: str


# Pydantic model for Dataset
class Dataset(BaseModel):
    id: int
    project_id: int
    name: str


# Pydantic model for Patient
class Patient(BaseModel):
    id: int
    project_id: int
    ext_patient_id: str
    ext_patient_url: str
    public_patient_id: Optional[str]
    sample_count: int

# Route to fetch all patients with sample counts
@app.get("/patients/", response_model=List[Patient])
async def get_patients():
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        # Query to fetch all patients with sample counts
        cursor.execute('''
            SELECT patients.id, patients.project_id, patients.ext_patient_id, patients.ext_patient_url,
                   patients.public_patient_id, COUNT(samples.id) AS sample_count
            FROM patients
            LEFT JOIN samples ON patients.id = samples.patient_id
            GROUP BY patients.id
            ORDER BY patients.id
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        # Convert rows to List[Patient] response
        patients = []
        for row in rows:
            patients.append({
                'id': row[0],
                'project_id': row[1],
                'ext_patient_id': row[2],
                'ext_patient_url': row[3],
                'public_patient_id': row[4],
                'sample_count': row[5]
            })
        
        return patients
    
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


# Route to fetch all projects and their statuses
@app.get("/projects/", response_model=List[Project])
async def get_projects():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, status FROM projects")
    rows = cursor.fetchall()
    conn.close()
    return [Project(id=row[0], name=row[1], status=row[2]) for row in rows]

# Route to fetch all datasets
@app.get("/datasets/", response_model=List[Dataset])
async def get_datasets():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, project_id, name FROM datasets")
    rows = cursor.fetchall()
    conn.close()
    return [Dataset(id=row[0], project_id=row[1], name=row[2]) for row in rows]

# Run the app using Uvicorn server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8888)

