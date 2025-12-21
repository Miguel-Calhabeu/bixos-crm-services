from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import io
import json
from pdf_parser import extract_records_from_bytes

app = FastAPI(title="SiSU PDF Parser API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.environ.get("DATABASE_URL")
NEXTJS_API_URL = os.environ.get("NEXTJS_API_URL", "http://localhost:3000")

def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL environment variable not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def process_job(job):
    job_id = job['id']
    storage_key = job['storage_key'] # This is the URL of the blob
    faculdade = job['Faculdade']
    ano = job['Ano']

    print(f"Processing job {job_id} for {faculdade} {ano}")

    try:
        # 1. Update status to parsing
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE imports SET status = 'parsing', updated_at = NOW() WHERE id = %s", (job_id,))
                conn.commit()

        # 2. Download PDF
        response = requests.get(storage_key)
        response.raise_for_status()
        pdf_bytes = response.content

        # 3. Parse PDF
        records = extract_records_from_bytes(pdf_bytes)
        
        # 4. Transform data for Next.js API
        # The schema expects: { leads: [ { Faculdade, Ano, Nome, Curso, Tipo, Periodo, Campus } ] }
        leads_payload = []
        for r in records:
            leads_payload.append({
                "Faculdade": faculdade,
                "Ano": int(ano),
                "Nome": r['nome'],
                "Curso": r['curso'],
                "Tipo": r['tipo'],
                "Periodo": r['periodo'],
                "Campus": r['campus']
            })

        # 5. Send to Next.js API
        if leads_payload:
            # We might need to chunk this if it's too large, but for now let's try sending all
            ingest_url = f"{NEXTJS_API_URL}/api/ingest/leads-raw"
            print(f"Sending {len(leads_payload)} leads to {ingest_url}")
            
            # Using a custom header if you want to add security later, but for now just simple POST
            ingest_res = requests.post(ingest_url, json={"leads": leads_payload})
            ingest_res.raise_for_status()
            
            ingest_data = ingest_res.json()
            print(f"Ingest result: {ingest_data}")
            
            stats = json.dumps({
                "extracted": len(records),
                "inserted": ingest_data.get("inserted", 0),
                "skipped": ingest_data.get("skipped", 0),
                "failed": ingest_data.get("failed", 0)
            })
        else:
            stats = json.dumps({"extracted": 0, "inserted": 0, "skipped": 0})

        # 6. Update status to completed
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE imports SET status = 'completed', stats_json = %s, updated_at = NOW() WHERE id = %s",
                    (stats, job_id)
                )
                conn.commit()
        
        print(f"Job {job_id} completed successfully")

    except Exception as e:
        print(f"Job {job_id} failed: {e}")
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE imports SET status = 'failed', error = %s, updated_at = NOW() WHERE id = %s",
                        (str(e), job_id)
                    )
                    conn.commit()
        except Exception as db_e:
            print(f"Failed to update error status for job {job_id}: {db_e}")

def process_pending_jobs_task():
    print("Starting background job processing...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Fetch pending jobs
                cur.execute("SELECT * FROM imports WHERE status = 'queued_parse' OR status = 'pending'")
                jobs = cur.fetchall()
        
        print(f"Found {len(jobs)} pending jobs")
        
        for job in jobs:
            process_job(job)
            
    except Exception as e:
        print(f"Error fetching jobs: {e}")

@app.get("/")
def read_root():
    return {"status": "ok", "message": "SiSU Parser API is running"}

@app.post("/process-jobs")
async def trigger_job_processing(background_tasks: BackgroundTasks):
    """
    Endpoint triggered by Next.js to start processing pending jobs.
    """
    background_tasks.add_task(process_pending_jobs_task)
    return {"status": "processing_started", "message": "Background task initiated"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)