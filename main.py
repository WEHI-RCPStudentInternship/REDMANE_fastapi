from fastapi import FastAPI, HTTPException
import sqlite3
from pydantic import BaseModel
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow all origins (for development, consider restricting to specific origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

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
    CREATE TABLE IF NOT EXISTS datasets_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dataset_id INTEGER NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL
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
    CREATE TABLE IF NOT EXISTS raw_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dataset_id INTEGER NOT NULL,
        path TEXT,
        UNIQUE(dataset_id, path),
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
from fastapi import FastAPI, HTTPException
import sqlite3
from pydantic import BaseModel
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow all origins (for development, consider restricting to specific origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

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
    CREATE TABLE IF NOT EXISTS datasets_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dataset_id INTEGER NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL
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
    CREATE TABLE IF NOT EXISTS raw_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dataset_id INTEGER NOT NULL,
        path TEXT,
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

class DatasetMetadata(BaseModel):
    id: int
    dataset_id: int
    key: str
    value: str

class DatasetWithMetadata(Dataset):
    metadata: List[DatasetMetadata] = []


# Pydantic model for Patient
class Patient(BaseModel):
    id: int
    project_id: int
    ext_patient_id: str
    ext_patient_url: str
    public_patient_id: Optional[str]

# Pydantic model for Patient with sample count
class PatientWithSampleCount(Patient):
    sample_count: int

# Pydantic model for PatientMetadata
class PatientMetadata(BaseModel):
    id: int
    patient_id: int
    key: str
    value: str

# Pydantic model for Patient with Metadata
class PatientWithMetadata(Patient):
    metadata: List[PatientMetadata] = []

# Pydantic model for SampleMetadata
class SampleMetadata(BaseModel):
    id: int
    sample_id: int
    key: str
    value: str

# Pydantic model for Sample
class Sample(BaseModel):
    id: int
    patient_id: int
    ext_sample_id: str
    ext_sample_url: str
    metadata: List[SampleMetadata] = []
    patient: Patient

# Pydantic model for SampleWithoutPatient
class SampleWithoutPatient(BaseModel):
    id: int
    patient_id: int
    ext_sample_id: str
    ext_sample_url: str
    metadata: List[SampleMetadata] = []

class RawFileResponse(BaseModel):
    id: int
    path: str
    sample_id: Optional[str] = None
    ext_sample_id: Optional[str] = None
    sample_metadata: Optional[List[SampleMetadata]] = None

# Pydantic model for Patient with Samples
class PatientWithSamples(PatientWithMetadata):
    samples: List[SampleWithoutPatient] = []

# Pydantic model for RawFileMetadata
class RawFileMetadataCreate(BaseModel):
    metadata_key: str
    metadata_value: str

# Updated Pydantic model for RawFile with nested metadata
class RawFileCreate(BaseModel):
    dataset_id: int
    path: str
    metadata: Optional[List[RawFileMetadataCreate]] = []



@app.post("/add_raw_files/")
async def add_raw_files(raw_files: List[RawFileCreate]):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        # Insert raw_files and fetch their IDs
        raw_file_ids = []
        for raw_file in raw_files:
            cursor.execute('''
                INSERT INTO raw_files (dataset_id, path)
                VALUES (?, ?)
            ''', (raw_file.dataset_id, raw_file.path))
            raw_file_id = cursor.lastrowid
            raw_file_ids.append(raw_file_id)

            # Insert associated metadata for this raw_file
            if raw_file.metadata:
                for metadata in raw_file.metadata:
                    cursor.execute('''
                        INSERT INTO raw_files_metadata (raw_file_id, metadata_key, metadata_value)
                        VALUES (?, ?, ?)
                    ''', (raw_file_id, metadata.metadata_key, metadata.metadata_value))

        conn.commit()
        conn.close()
        return {"status": "success", "message": "Raw files and metadata added successfully"}

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all patients and their metadata for a project_id
@app.get("/patients_metadata/{patient_id}", response_model=List[PatientWithSamples])
async def get_patients_metadata(project_id: int,patient_id: int):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()


        if patient_id != 0:

            cursor.execute('''
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = ? and p.id = ?
                ORDER BY p.id
            ''', (project_id,patient_id,))
        else:
    
            cursor.execute('''
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = ?
                ORDER BY p.id
            ''', (project_id,))

        rows = cursor.fetchall()

        patients = []
        current_patient = None
        for row in rows:

            if not current_patient or current_patient['id'] != row[0]:

                if current_patient:
                    patients.append(current_patient)

                current_patient = {
                    'id': row[0],
                    'project_id': row[1],
                    'ext_patient_id': row[2],
                    'ext_patient_url': row[3],
                    'public_patient_id': row[4],
                    'samples': [],
                    'metadata': [] 
                }

            if row[5]:
                current_patient['metadata'].append({
                    'id': row[5],
                    'patient_id': row[0],
                    'key': row[6],
                    'value': row[7]
                })

        if current_patient:
            patients.append(current_patient)


        for patient in patients:
            cursor.execute('''
                SELECT s.id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id, sm.key, sm.value
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                WHERE s.patient_id = ?
                ORDER BY s.id
            ''', (patient['id'],))

            sample_rows = cursor.fetchall()
            current_sample = None
            for sample_row in sample_rows:
                if not current_sample or current_sample['id'] != sample_row[0]:
                    if current_sample:
                        patient['samples'].append(current_sample)
                    current_sample = {
                        'id': sample_row[0],
                        'patient_id': sample_row[1],
                        'ext_sample_id': sample_row[2],
                        'ext_sample_url': sample_row[3],
                        'metadata': []
                    }
                if sample_row[4]:
                    current_sample['metadata'].append({
                        'id': sample_row[4],
                        'sample_id': sample_row[0],
                        'key': sample_row[5],
                        'value': sample_row[6]
                    })
            if current_sample:
                patient['samples'].append(current_sample)

        conn.close()

        return patients

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all samples and metadata for a project_id and include patient information
@app.get("/samples/{sample_id}", response_model=List[Sample])
async def get_samples_per_patient(sample_id: int, project_id: int):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        if sample_id != 0:
            cursor.execute('''
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = ? and s.id = ?
                ORDER BY s.id, sm.id
            ''', (project_id,sample_id,))
        else:
            cursor.execute('''
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = ?
                ORDER BY s.id, sm.id
            ''', (project_id,))
 


        rows = cursor.fetchall()
        conn.close()

        samples = []
        current_sample = None
        for row in rows:
            if not current_sample or current_sample['id'] != row[0]:
                if current_sample:
                    samples.append(current_sample)
                current_sample = {
                    'id': row[0],
                    'patient_id': row[1],
                    'ext_sample_id': row[2],
                    'ext_sample_url': row[3],
                    'metadata': [],
                    'patient': {
                        'id': row[7],
                        'project_id': row[8],
                        'ext_patient_id': row[9],
                        'ext_patient_url': row[10],
                        'public_patient_id': row[11]
                    }
                }

            if row[4]:  # Check if metadata exists
                current_sample['metadata'].append({
                    'id': row[4],
                    'sample_id': row[0],
                    'key': row[5],
                    'value': row[6]
                })

        if current_sample:
            samples.append(current_sample)

        return samples

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all patients with sample counts
@app.get("/patients/{patient_id}", response_model=List[PatientWithSampleCount])
async def get_patients(project_id: int, patient_id: int):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        # Query to fetch all patients with sample counts
        cursor.execute('''
            SELECT patients.id, patients.project_id, patients.ext_patient_id, patients.ext_patient_url,
                   patients.public_patient_id, COUNT(samples.id) AS sample_count
            FROM patients
            LEFT JOIN samples ON patients.id = samples.patient_id
            WHERE patients.project_id = ?
            GROUP BY patients.id
            ORDER BY patients.id
        ''', (project_id,))

        
        rows = cursor.fetchall()
        conn.close()
        
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
@app.get("/datasets/{dataset_id}", response_model=List[Dataset])
async def get_datasets(dataset_id: int, project_id: int):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    if dataset_id != 0:
        cursor.execute('''SELECT id, project_id, name FROM datasets where project_id = ? and dataset_id = ?''', (project_id,dataset_id,))
    else:
        cursor.execute('''SELECT id, project_id, name FROM datasets where project_id = ?''', (project_id,))


    rows = cursor.fetchall()
    conn.close()
    return [Dataset(id=row[0], project_id=row[1], name=row[2]) for row in rows]


# Endpoint to fetch dataset details and metadata by dataset_id
@app.get("/datasets_with_metadata/{dataset_id}", response_model=DatasetWithMetadata)
async def get_dataset_with_metadata(dataset_id: int, project_id: int):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        # Fetch dataset details
        cursor.execute('''
            SELECT id, project_id, name
            FROM datasets
            WHERE id = ? AND project_id = ?
        ''', (dataset_id, project_id))
        dataset_row = cursor.fetchone()
        
        if not dataset_row:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # Fetch dataset metadata
        cursor.execute('''
            SELECT id, dataset_id, key, value
            FROM datasets_metadata
            WHERE dataset_id = ?
        ''', (dataset_id,))
        metadata_rows = cursor.fetchall()
        
        conn.close()
        
        dataset = {
            "id": dataset_row[0],
            "project_id": dataset_row[1],
            "name": dataset_row[2],
            "metadata": [{"id": row[0], "dataset_id": row[1], "key": row[2], "value": row[3]} for row in metadata_rows]
        }

        return dataset

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")



# Run the app using Uvicorn server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8888)


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

class DatasetMetadata(BaseModel):
    id: int
    dataset_id: int
    key: str
    value: str

class DatasetWithMetadata(Dataset):
    metadata: List[DatasetMetadata] = []


# Pydantic model for Patient
class Patient(BaseModel):
    id: int
    project_id: int
    ext_patient_id: str
    ext_patient_url: str
    public_patient_id: Optional[str]

# Pydantic model for Patient with sample count
class PatientWithSampleCount(Patient):
    sample_count: int

# Pydantic model for PatientMetadata
class PatientMetadata(BaseModel):
    id: int
    patient_id: int
    key: str
    value: str

# Pydantic model for Patient with Metadata
class PatientWithMetadata(Patient):
    metadata: List[PatientMetadata] = []

# Pydantic model for SampleMetadata
class SampleMetadata(BaseModel):
    id: int
    sample_id: int
    key: str
    value: str

# Pydantic model for Sample
class Sample(BaseModel):
    id: int
    patient_id: int
    ext_sample_id: str
    ext_sample_url: str
    metadata: List[SampleMetadata] = []
    patient: Patient

# Pydantic model for SampleWithoutPatient
class SampleWithoutPatient(BaseModel):
    id: int
    patient_id: int
    ext_sample_id: str
    ext_sample_url: str
    metadata: List[SampleMetadata] = []



# Pydantic model for Patient with Samples
class PatientWithSamples(PatientWithMetadata):
    samples: List[SampleWithoutPatient] = []


# Route to fetch all patients and their metadata for a project_id
@app.get("/patients_metadata/{patient_id}", response_model=List[PatientWithSamples])
async def get_patients_metadata(project_id: int,patient_id: int):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()


        if patient_id != 0:

            cursor.execute('''
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = ? and p.id = ?
                ORDER BY p.id
            ''', (project_id,patient_id,))
        else:
    
            cursor.execute('''
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = ?
                ORDER BY p.id
            ''', (project_id,))

        rows = cursor.fetchall()

        patients = []
        current_patient = None
        for row in rows:

            if not current_patient or current_patient['id'] != row[0]:

                if current_patient:
                    patients.append(current_patient)

                current_patient = {
                    'id': row[0],
                    'project_id': row[1],
                    'ext_patient_id': row[2],
                    'ext_patient_url': row[3],
                    'public_patient_id': row[4],
                    'samples': [],
                    'metadata': [] 
                }

            if row[5]:
                current_patient['metadata'].append({
                    'id': row[5],
                    'patient_id': row[0],
                    'key': row[6],
                    'value': row[7]
                })

        if current_patient:
            patients.append(current_patient)


        for patient in patients:
            cursor.execute('''
                SELECT s.id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id, sm.key, sm.value
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                WHERE s.patient_id = ?
                ORDER BY s.id
            ''', (patient['id'],))

            sample_rows = cursor.fetchall()
            current_sample = None
            for sample_row in sample_rows:
                if not current_sample or current_sample['id'] != sample_row[0]:
                    if current_sample:
                        patient['samples'].append(current_sample)
                    current_sample = {
                        'id': sample_row[0],
                        'patient_id': sample_row[1],
                        'ext_sample_id': sample_row[2],
                        'ext_sample_url': sample_row[3],
                        'metadata': []
                    }
                if sample_row[4]:
                    current_sample['metadata'].append({
                        'id': sample_row[4],
                        'sample_id': sample_row[0],
                        'key': sample_row[5],
                        'value': sample_row[6]
                    })
            if current_sample:
                patient['samples'].append(current_sample)

        conn.close()

        return patients

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all samples and metadata for a project_id and include patient information
@app.get("/samples/{sample_id}", response_model=List[Sample])
async def get_samples_per_patient(sample_id: int, project_id: int):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        if sample_id != 0:
            cursor.execute('''
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = ? and s.id = ?
                ORDER BY s.id, sm.id
            ''', (project_id,sample_id,))
        else:
            cursor.execute('''
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = ?
                ORDER BY s.id, sm.id
            ''', (project_id,))
 


        rows = cursor.fetchall()
        conn.close()

        samples = []
        current_sample = None
        for row in rows:
            if not current_sample or current_sample['id'] != row[0]:
                if current_sample:
                    samples.append(current_sample)
                current_sample = {
                    'id': row[0],
                    'patient_id': row[1],
                    'ext_sample_id': row[2],
                    'ext_sample_url': row[3],
                    'metadata': [],
                    'patient': {
                        'id': row[7],
                        'project_id': row[8],
                        'ext_patient_id': row[9],
                        'ext_patient_url': row[10],
                        'public_patient_id': row[11]
                    }
                }

            if row[4]:  # Check if metadata exists
                current_sample['metadata'].append({
                    'id': row[4],
                    'sample_id': row[0],
                    'key': row[5],
                    'value': row[6]
                })

        if current_sample:
            samples.append(current_sample)

        return samples

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all patients with sample counts
@app.get("/patients/{patient_id}", response_model=List[PatientWithSampleCount])
async def get_patients(project_id: int, patient_id: int):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        # Query to fetch all patients with sample counts
        cursor.execute('''
            SELECT patients.id, patients.project_id, patients.ext_patient_id, patients.ext_patient_url,
                   patients.public_patient_id, COUNT(samples.id) AS sample_count
            FROM patients
            LEFT JOIN samples ON patients.id = samples.patient_id
            WHERE patients.project_id = ?
            GROUP BY patients.id
            ORDER BY patients.id
        ''', (project_id,))

        
        rows = cursor.fetchall()
        conn.close()
        
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
@app.get("/datasets/{dataset_id}", response_model=List[Dataset])
async def get_datasets(dataset_id: int, project_id: int):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    if dataset_id != 0:
        cursor.execute('''SELECT id, project_id, name FROM datasets where project_id = ? and dataset_id = ?''', (project_id,dataset_id,))
    else:
        cursor.execute('''SELECT id, project_id, name FROM datasets where project_id = ?''', (project_id,))


    rows = cursor.fetchall()
    conn.close()
    return [Dataset(id=row[0], project_id=row[1], name=row[2]) for row in rows]


# Endpoint to fetch dataset details and metadata by dataset_id
@app.get("/datasets_with_metadata/{dataset_id}", response_model=DatasetWithMetadata)
async def get_dataset_with_metadata(dataset_id: int, project_id: int):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        # Fetch dataset details
        cursor.execute('''
            SELECT id, project_id, name
            FROM datasets
            WHERE id = ? AND project_id = ?
        ''', (dataset_id, project_id))
        dataset_row = cursor.fetchone()
        
        if not dataset_row:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # Fetch dataset metadata
        cursor.execute('''
            SELECT id, dataset_id, key, value
            FROM datasets_metadata
            WHERE dataset_id = ?
        ''', (dataset_id,))
        metadata_rows = cursor.fetchall()
        
        conn.close()
        
        dataset = {
            "id": dataset_row[0],
            "project_id": dataset_row[1],
            "name": dataset_row[2],
            "metadata": [{"id": row[0], "dataset_id": row[1], "key": row[2], "value": row[3]} for row in metadata_rows]
        }

        return dataset

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.get("/raw_files_with_metadata/{dataset_id}", response_model=List[RawFileResponse])
async def get_raw_files_with_metadata(dataset_id: int):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Query to get raw files and their associated metadata
    query = """
    SELECT rf.id, rf.path, rfm.metadata_value AS sample_id, s.ext_sample_id
    FROM raw_files rf
    LEFT JOIN raw_files_metadata rfm ON rf.id = rfm.raw_file_id
    LEFT JOIN samples s ON rfm.metadata_value = s.id
    WHERE rf.dataset_id = ? AND rfm.metadata_key = 'sample_id'
    """
    cursor.execute(query, (dataset_id,))
    raw_files = cursor.fetchall()

    response = []
    
    for raw_file in raw_files:
        raw_file_id, path, sample_id, ext_sample_id = raw_file
        
        # Fetch sample metadata
        cursor.execute("SELECT id, sample_id, key, value FROM samples_metadata WHERE sample_id = ?", (sample_id,))
        sample_metadata_rows = cursor.fetchall()

        sample_metadata_list = []
        for row in sample_metadata_rows:
            sample_metadata_list.append({
                'id': row[0],
                'sample_id': row[1],
                'key': row[2],
                'value': row[3]
                

            }) 
        print(sample_metadata_list)


        response.append(RawFileResponse(
            id=raw_file_id,
            path=path,
            sample_id=sample_id,
            ext_sample_id=ext_sample_id,
            sample_metadata=sample_metadata_list
        ))

    conn.close()
    return response

# Run the app using Uvicorn server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8888)

