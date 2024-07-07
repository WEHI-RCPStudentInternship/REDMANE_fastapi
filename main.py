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
        ext_sample_org TEXT,
        ext_org_batch TEXT,
        tissue TEXT,
        sample_date TEXT,
        internal_batch TEXT,
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
        sample_id INTEGER,
        FOREIGN KEY (dataset_id) REFERENCES datasets(id),
        FOREIGN KEY (sample_id) REFERENCES samples(id)
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

