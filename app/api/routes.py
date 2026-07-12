import os
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, File, UploadFile, Form, Depends
from fastapi.responses import RedirectResponse
import json
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import Error
from app.schemas.schemas import (
    FileCreate,
    Project,
    Dataset,
    DatasetMetadata,
    DatasetWithMetadata,
    Patient,
    PatientWithSampleCount,
    PatientMetadata,
    PatientWithMetadata,
    SampleMetadata,
    Sample,
    SampleWithoutPatient,
    FileResponse,
    PatientWithSamples,
    FileMetadataCreate,
    MetadataUpdate,
    DatasetSummary,
    Totals,
    ProjectSummary,
    FileWithMetadata
)
from app.routers.auth import verify_token

DB_NAME = os.getenv("DB_NAME", "readmedatabase")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")

router = APIRouter()


def get_connection():
    """
    Helper function to get a new connection to your PostgreSQL database.
    """
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


@router.get("/")
async def root():
    return RedirectResponse(url="/projects")


@router.get("/projects/", response_model=List[Project])
async def get_projects(token: dict = Depends(verify_token)):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, status FROM projects")
        rows = cursor.fetchall()
        conn.close()
        return [Project(id=row[0], name=row[1], status=row[2]) for row in rows]
    except Error as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/projects/{project_id}/summary", response_model=ProjectSummary)
def get_project_summary(project_id: int, token: dict = Depends(verify_token)):
    """
    Summarize a project's datasets:
      - file_count          : DISTINCT files per dataset
      - patient_count       : DISTINCT patient_id values in files_metadata
      - sample_count        : DISTINCT sample_id values in files_metadata
      - total_size_kb       : SUM of file_size (text) cast to numeric (assumed KB)
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT name FROM projects WHERE id = %s", (project_id,))
        row = cur.fetchone()
        project_name = row[0] if row else None

        sql = """
        SELECT
          d.id   AS dataset_id,
          d.name AS dataset_name,
          COUNT(DISTINCT f.id) AS file_count,
          COUNT(DISTINCT CASE WHEN fm.metadata_key = 'patient_id' THEN fm.metadata_value END) AS patient_count,
          COUNT(DISTINCT CASE WHEN fm.metadata_key = 'sample_id'  THEN fm.metadata_value END) AS sample_count,
          COALESCE(
            SUM(
              (regexp_replace(fm.metadata_value, '\\s', '', 'g'))::numeric
            )
            FILTER (
              WHERE fm.metadata_key = 'file_size'
                AND fm.metadata_value ~ '^[[:space:]]*[0-9]+(\\.[0-9]+)?[[:space:]]*$'
            ),
            0
          )::bigint AS total_size_kb
        FROM datasets d
        LEFT JOIN files f          ON f.dataset_id = d.id
        LEFT JOIN files_metadata fm ON fm.file_id   = f.id
        WHERE d.project_id = %s
        GROUP BY d.id, d.name
        ORDER BY d.name;
        """
        cur.execute(sql, (project_id,))
        rows = cur.fetchall()

        datasets = [
            DatasetSummary(
                dataset_id=r[0],
                dataset_name=r[1],
                file_count=int(r[2]),
                patient_count=int(r[3]),
                sample_count=int(r[4]),
                total_size_kb=int(r[5]),
            )
            for r in rows
        ]

        totals = Totals(
            file_count=sum(d.file_count for d in datasets),
            patient_count=sum(d.patient_count for d in datasets),
            sample_count=sum(d.sample_count for d in datasets),
            total_size_kb=sum(d.total_size_kb for d in datasets),
        )

        return ProjectSummary(
            project_id=project_id,
            project_name=project_name,
            totals=totals,
            datasets=datasets,
        )

    except Error as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        if conn:
            conn.close()


@router.post("/datasets/")
async def create_dataset(
    project_id: int = Form(...),
    name: str = Form(...),
    abstract: str = Form(...),
    site: str = Form(...),
    location: Optional[str] = Form(None),
    raw_files: Optional[str] = Form(None),
    processed_files: Optional[str] = Form(None),
    summary_files: Optional[str] = Form(None),
    readme_files: Optional[str] = Form(None),
    token: dict = Depends(verify_token)
):
    """
    Create a new dataset entry and insert optional metadata into datasets_metadata.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO datasets (project_id, name, abstract, site, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            RETURNING id
            """,
            (project_id, name, abstract, site)
        )
        dataset_id = cursor.fetchone()[0]

        metadata_entries = []
        if raw_files:
            metadata_entries.append(('raw_files', raw_files))
        if processed_files:
            metadata_entries.append(('processed_files', processed_files))
        if summary_files:
            metadata_entries.append(('summary_files', summary_files))
        if readme_files:
            metadata_entries.append(('readme_files', readme_files))
        if location:
            metadata_entries.append(('location', location))

        for key, value in metadata_entries:
            cursor.execute(
                """
                INSERT INTO datasets_metadata (dataset_id, key, value)
                VALUES (%s, %s, %s)
                """,
                (dataset_id, key, value)
            )

        conn.commit()

        return {
            "status": "success",
            "dataset_id": dataset_id,
            "message": f"Dataset '{name}' created successfully"
        }

    except Error as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/datasets/", response_model=List[Dataset])
async def get_datasets(
    project_id: Optional[int] = Query(None, description="Filter by project ID"),
    dataset_id: Optional[int] = Query(None, description="Filter by dataset ID"),
    token: dict = Depends(verify_token)
):
    """
    Fetch datasets, optionally filtered by project_id and/or dataset_id.
    Includes rank_in_project computed per project.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            SELECT * FROM (
                SELECT id, project_id, name, abstract, created_at, site,
                       ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY id ASC) AS rank_in_project
                FROM datasets
            ) ranked
            WHERE 1=1
        """
        params = []

        if project_id is not None:
            query += " AND project_id = %s"
            params.append(project_id)

        if dataset_id is not None:
            query += " AND id = %s"
            params.append(dataset_id)

        query += " ORDER BY project_id ASC, id ASC;"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [
            Dataset(
                id=row[0],
                project_id=row[1],
                name=row[2],
                abstract=row[3],
                created_at=row[4],
                site=row[5],
                rank_in_project=row[6]
            )
            for row in rows
        ]

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/datasets_with_metadata/{dataset_id}")
async def get_dataset_with_metadata(dataset_id: int, token: dict = Depends(verify_token)):
    """
    Fetch dataset details (and its metadata) for the given dataset_id.
    Includes rank_in_project computed per project.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            SELECT * FROM (
                SELECT id, project_id, name, abstract, created_at, site,
                       ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY id ASC) AS rank_in_project
                FROM datasets
            ) ranked
            WHERE id = %s;
        """
        cursor.execute(query, (dataset_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Dataset not found for id {dataset_id}")

        dataset = {
            "id": row[0],
            "project_id": row[1],
            "name": row[2],
            "abstract": row[3],
            "created_at": row[4],
            "site": row[5],
            "rank_in_project": row[6],
        }

        cursor.execute(
            "SELECT key, value FROM datasets_metadata WHERE dataset_id = %s;",
            (dataset_id,),
        )
        metadata_rows = cursor.fetchall()
        metadata = [{"key": m[0], "value": m[1]} for m in metadata_rows]

        conn.close()
        return {**dataset, "metadata": metadata}

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/patients/", response_model=List[PatientWithSampleCount])
async def get_patients(
    project_id: Optional[int] = Query(None, description="Filter by project ID"),
    token: dict = Depends(verify_token)
):
    """
    Fetch all patients (optionally filtered by project_id) with a count of how many samples they have.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url,
                   p.public_patient_id, COUNT(s.id) AS sample_count
            FROM patients p
            LEFT JOIN samples s ON p.id = s.patient_id
        """
        params = []

        if project_id is not None:
            query += " WHERE p.project_id = %s"
            params.append(project_id)

        query += " GROUP BY p.id ORDER BY p.id"

        cursor.execute(query, params)
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

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/patients_metadata/{patient_id}", response_model=List[PatientWithSamples])
async def get_patients_metadata(project_id: int, patient_id: int, token: dict = Depends(verify_token)):
    """
    Fetch patients (and their samples + metadata) for a given project_id.
    If patient_id == 0, fetch all patients; otherwise, fetch the specified patient.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if patient_id != 0:
            cursor.execute(
                """
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = %s AND p.id = %s
                ORDER BY p.id
                """,
                (project_id, patient_id)
            )
        else:
            cursor.execute(
                """
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = %s
                ORDER BY p.id
                """,
                (project_id,)
            )

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
            cursor.execute(
                """
                SELECT s.id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id, sm.key, sm.value
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                WHERE s.patient_id = %s
                ORDER BY s.id
                """,
                (patient['id'],)
            )

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

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/samples/{sample_id}", response_model=List[Sample])
async def get_samples_per_patient(sample_id: int, project_id: int, token: dict = Depends(verify_token)):
    """
    Fetch samples (and their metadata) for a given project_id, optionally filtering by sample_id.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if sample_id != 0:
            cursor.execute(
                """
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = %s AND s.id = %s
                ORDER BY s.id, sm.id
                """,
                (project_id, sample_id)
            )
        else:
            cursor.execute(
                """
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = %s
                ORDER BY s.id, sm.id
                """,
                (project_id,)
            )

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
            if row[4]:
                current_sample['metadata'].append({
                    'id': row[4],
                    'sample_id': row[0],
                    'key': row[5],
                    'value': row[6]
                })

        if current_sample:
            samples.append(current_sample)

        return samples

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.post("/add_files/")
async def add_files(files: List[FileCreate], token: dict = Depends(verify_token)):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        file_ids = []
        for file in files:
            cursor.execute(
                """
                INSERT INTO files (dataset_id, path, file_type)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (file.dataset_id, file.path, file.file_type)
            )
            file_id = cursor.fetchone()[0]
            file_ids.append(file_id)

            if file.metadata:
                for metadata in file.metadata:
                    cursor.execute(
                        """
                        INSERT INTO files_metadata (file_id, metadata_key, metadata_value)
                        VALUES (%s, %s, %s)
                        """,
                        (file_id, metadata.metadata_key, metadata.metadata_value)
                    )

        conn.commit()
        conn.close()
        return {"status": "success", "message": "Files and metadata added successfully"}

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/files_with_metadata/{dataset_id}", response_model=List[FileResponse])
async def get_files_with_metadata(dataset_id: int, token: dict = Depends(verify_token)):
    """
    Fetch files within a dataset, along with any related sample metadata.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            SELECT f.id, f.path, fm.metadata_value AS sample_id, s.ext_sample_id
            FROM files f
            LEFT JOIN files_metadata fm ON f.id = fm.file_id
            LEFT JOIN samples s ON fm.metadata_value = CAST(s.id AS TEXT)
            WHERE f.dataset_id = %s AND fm.metadata_key = 'sample_id'
        """
        cursor.execute(query, (dataset_id,))
        files = cursor.fetchall()

        response = []

        for (file_id, path, sample_id, ext_sample_id) in files:
            cursor.execute(
                """
                SELECT id, sample_id, key, value
                FROM samples_metadata
                WHERE sample_id = %s
                """,
                (sample_id,)
            )
            sample_metadata_rows = cursor.fetchall()
            sample_metadata_list = []
            for row in sample_metadata_rows:
                sample_metadata_list.append({
                    'id': row[0],
                    'sample_id': row[1],
                    'key': row[2],
                    'value': row[3]
                })

            response.append(FileResponse(
                id=file_id,
                path=path,
                sample_id=int(sample_id) if sample_id is not None else None,
                ext_sample_id=ext_sample_id,
                sample_metadata=sample_metadata_list
            ))

        conn.close()
        return response

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.put("/datasets_metadata/size_update", response_model=MetadataUpdate)
def update_metadata(update: MetadataUpdate, token: dict = Depends(verify_token)):
    """
    Update specific metadata fields in the datasets_metadata table.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if update.file_size:
            cursor.execute(
                "SELECT id FROM datasets_metadata WHERE key = 'file_extension_size_of_all_files' AND dataset_id = %s",
                (update.dataset_id,)
            )
            record = cursor.fetchone()
            if record:
                cursor.execute("UPDATE datasets_metadata SET value = %s WHERE id = %s", (update.file_size, record[0]))
            else:
                cursor.execute(
                    "INSERT INTO datasets_metadata (dataset_id, key, value) VALUES (%s, 'file_extension_size_of_all_files', %s)",
                    (update.dataset_id, update.file_size)
                )

        if update.last_size_update:
            cursor.execute(
                "SELECT id FROM datasets_metadata WHERE key = 'last_size_update' AND dataset_id = %s",
                (update.dataset_id,)
            )
            record = cursor.fetchone()
            if record:
                cursor.execute("UPDATE datasets_metadata SET value = %s WHERE id = %s", (update.last_size_update, record[0]))
            else:
                cursor.execute(
                    "INSERT INTO datasets_metadata (dataset_id, key, value) VALUES (%s, 'last_size_update', %s)",
                    (update.dataset_id, update.last_size_update)
                )

        conn.commit()
        conn.close()
        return update

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


def _process_files(cursor, file_list: list, file_type_name: str, dataset_id: int) -> tuple:
    """
    Helper function for file metadata upload which reads a list of files for a specific type
    (raw/processed/summarised) and returns the results.
    """
    if not file_list:
        return (0, 0)

    count = 0
    total_size = 0

    for file_detail in file_list:
        count += 1
        total_size += int(file_detail.get('file_size', 0))

        cursor.execute(
            """
            INSERT INTO files (dataset_id, path, file_type)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (dataset_id, file_detail['directory'], file_type_name)
        )

        file_id = cursor.fetchone()[0]
        organization = file_detail.get('organization', 'Unknown')

        metadata_to_insert = [
            (file_id, 'file_name', file_detail.get('file_name')),
            (file_id, 'file_size', str(file_detail.get('file_size', 0))),
            (file_id, 'organization', organization),
            (file_id, 'patient_id', file_detail.get('patient_id')),
            (file_id, 'sample_id', file_detail.get('sample_id')),
        ]

        execute_values(
            cursor,
            "INSERT INTO files_metadata (file_id, metadata_key, metadata_value) VALUES %s",
            metadata_to_insert
        )

    return count, total_size


@router.post("/ingest/upload_file_metadata")
async def upload_file_metadata(
    dataset_id: int = Form(...),
    file: UploadFile = File(...),
    token: dict = Depends(verify_token)
):
    """
    Read from json file containing metadata information and upload into database.
    Returns a summary of upload information.
    """
    try:
        contents = await file.read()
        decoded_contents = contents.decode('utf-8')
        ingestion_data = json.loads(decoded_contents)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload a valid JSON file.")

    conn = None

    try:
        files_by_type = ingestion_data.get('data').get('files')
        file_size_unit = ingestion_data.get('data').get('file_size_unit', 'units')

        summary = {
            "raw": {"count": 0, "total_size": 0},
            "processed": {"count": 0, "total_size": 0},
            "summarised": {"count": 0, "total_size": 0}
        }

        conn = get_connection()
        cursor = conn.cursor()

        for file_type in summary.keys():
            file_list = files_by_type.get(file_type, [])
            count, total_size = _process_files(
                cursor=cursor,
                file_list=file_list,
                file_type_name=file_type,
                dataset_id=dataset_id
            )
            summary[file_type]["count"] = count
            summary[file_type]["total_size"] = total_size

        conn.commit()

        return {
            "status": "success",
            "message": f"Successfully ingested files for dataset ID {dataset_id}",
            "summary": {
                "file_size_unit": file_size_unit,
                "raw_files": summary["raw"],
                "processed_files": summary["processed"],
                "summarised_files": summary["summarised"]
            }
        }
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Error: The uploaded JSON file is missing an expected key: {e}")
    except Error as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database transaction failed: {e}")
    finally:
        if conn:
            conn.close()


@router.get("/dataset_files_metadata/{dataset_id}", response_model=List[FileWithMetadata])
async def get_dataset_files_metadata(dataset_id: int, token: dict = Depends(verify_token)):
    """
    Fetch files within a dataset, along with the metadata for each file.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            SELECT file_id, file_type, metadata_key, metadata_value 
            FROM files INNER JOIN files_metadata ON files.id = files_metadata.file_id
            WHERE files.dataset_id = %s
        """
        cursor.execute(query, (dataset_id,))
        metadata = cursor.fetchall()

        files_map = {}

        for (file_id, file_type, metadata_key, metadata_value) in metadata:
            if file_id not in files_map:
                files_map[file_id] = {
                    "id": file_id,
                    "file_type": file_type,
                    "metadata": []
                }
            if metadata_key is not None:
                files_map[file_id]["metadata"].append(
                    {"metadata_key": metadata_key, "metadata_value": metadata_value}
                )

        conn.close()
        return list(files_map.values())

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/{datasetId}/external-links")
async def get_dataset_external_links(datasetId: str, token: dict = Depends(verify_token)):
    """
    Get only metadata entries where key contains "url".
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            SELECT key, value 
            FROM datasets_metadata 
            WHERE dataset_id = %s 
            AND key LIKE %s
            AND key NOT LIKE %s
            ORDER BY key
        """
        cursor.execute(query, (datasetId, '%url%', '%_label'))
        link_rows = cursor.fetchall()

        links = []
        for row in link_rows:
            key = row[0]
            url = row[1]

            if "url" in key.lower() and not url.startswith("https://"):
                raise HTTPException(
                    status_code=400,
                    detail=f"URL for key '{key}' must start with 'https://'"
                )

            links.append({"key": key, "url": url})

        conn.close()

        return {
            "datasetId": datasetId,
            "links": links
        }

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")