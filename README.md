# REDMANE Data Registry: FastAPI Backend

## Overview
The REDMANE Data Registry is a web application using a FastAPI backend with a PostgreSQL database.

## Related Repositories
- **Docker Orchestration**: [REDMANE_Docker](https://github.com/WEHI-RCPStudentInternship/REDMANE_Docker)
- **Frontend**: [REDMANE_react.js](https://github.com/WEHI-RCPStudentInternship/REDMANE_react.js)
- **Database Schemas & Seed Data**: [REDMANE_fastapi_public_data](https://github.com/WEHI-RCPStudentInternship/REDMANE_fastapi_public_data)
- **Metadata File Creation**: [REDMANE-metadata-generator-with-RO-Crate](https://github.com/WEHI-RCPStudentInternship/REDMANE-metadata-generator-with-RO-Crate)

## Features
- **Authentication**: Keycloak SSO integration with JWT token verification
- **Dataset Management**: Manage datasets with options to view all datasets or a single dataset
- **Patient Management**: View and manage patient information
- **Project Management**: View all projects or a single project, with a dashboard overview
- **File Tracking**: Track and manage raw data files with metadata

## Project Structure
```plaintext
REDMANE_fastapi/
├── app/
│   ├── api/
│   │   ├── __init__.py             # Initialises the API package
│   │   └── routes.py               # Defines API endpoints
│   ├── schemas/
│   │   ├── __init__.py             # Initialises the schemas package
│   │   └── schemas.py              # Defines Pydantic models for data validation
│   ├── routers/
│   │   ├── __init__.py             # Initialises the routers package
│   │   └── auth.py                 # Keycloak SSO authentication (JWT token verification)
│   ├── __init__.py                 # Currently empty init file
│   └── main.py                     # Entry point for the FastAPI application
├── data/
│   ├── sample_data/                # Sample datasets and scripts for testing
│   │   ├── clear_patients_and_samples.sh  # Script to clear sample data
│   │   ├── import_onj_patients.py  # Script to import ONJ patients
│   │   ├── import_onj_samples.py   # Script to import ONJ samples
│   │   ├── import_rmh_patients.py  # Script to import RMH patients
│   │   ├── redcap_onj.csv          # Sample ONJ patient data
│   │   ├── redcap_onj_samples.csv  # Sample ONJ sample data
│   │   └── redcap_rmh.csv          # Sample RMH patient data
│   └── sample_files/               # Sample files for raw data tracking
│       └── tracker/                # Folder for tracking scripts and raw data
│           ├── create_counts_file_big.py   # Script for processing large count files
│           ├── create_counts_file_size.py  # Script for calculating file size
│           ├── create_fastq_size.py        # Script for FASTQ size processing
│           ├── file_report.py              # Script for generating file reports
│           ├── scrnaseq/raw/       # scRNA-seq raw data files
│           │   └── random_file_2.fastq   # Example FASTQ file
│           └── westn/raw/          # WES raw data files
│               └── *.fastq         # Example FASTQ files for testing
├── LICENSE
├── README.md
├── .gitignore
├── requirements.txt                # Python dependencies
└── output.json                     # Sample metadata for ingestion testing
```

## Installation

### Dockerised Deployment (Recommended)
This backend is designed to run as part of the REDMANE Docker stack. See the [REDMANE_Docker](https://github.com/WEHI-RCPStudentInternship/REDMANE_Docker) repo for deployment instructions.

The database schema and seed data are managed in the [REDMANE_fastapi_public_data](https://github.com/WEHI-RCPStudentInternship/REDMANE_fastapi_public_data) repo, which should be cloned into `data/REDMANE_fastapi_public_data/` within this repo.

### Local Development (Without Docker)
1. **Create a Python virtual environment:**
```bash
   python3 -m venv env
   source env/bin/activate
```

2. **Install dependencies:**
```bash
   pip install -r requirements.txt
```

3. **Set up PostgreSQL:**
   Ensure PostgreSQL is installed and create the database:
```sql
   CREATE DATABASE readmedatabase;
```
   Then run the schema from `data/REDMANE_fastapi_public_data/readmedatabase.sql`.

4. **Run the server:**
```bash
   uvicorn app.main:app --reload --port 8888
```

   Note: The database connection in `app/api/routes.py` defaults to `localhost:5432`. Update `DB_HOST` if your PostgreSQL instance is elsewhere.
