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
SUPABASE_STORAGE_BASE_URL = os.environ.get("SUPABASE_STORAGE_BASE_URL")

def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL environment variable not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def process_job(job):
    job_id = job['id']
    storage_key = job['storage_key']
    
    # Handle storage key to full URL using Supabase Storage
    if not storage_key.startswith("http"):
        if SUPABASE_STORAGE_BASE_URL:
            # Ensure no double slashes if both end/start with /
            base = SUPABASE_STORAGE_BASE_URL.rstrip('/')
            path = storage_key.lstrip('/')
            storage_key = f"{base}/{path}"
        else:
            print(f"Warning: Job {job_id} has relative storage_key '{storage_key}' but SUPABASE_STORAGE_BASE_URL is not set.")

    faculdade = job['Faculdade']
    ano = job['Ano']

    print(f"Processing job {job_id} for {faculdade} {ano}. URL: {storage_key}")

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
        
        # 4. Insert directly into Database (leads_raw)
        inserted_count = 0
        skipped_count = 0
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                insert_query = """
                    INSERT INTO public.leads_raw (
                        "Faculdade", "Ano", "Nome", "Curso", "Tipo", "Periodo", "Campus"
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ("Faculdade", "Ano", "Nome") DO NOTHING
                """
                
                # Prepare batch data
                batch_data = [
                    (
                        faculdade,
                        int(ano),
                        r['nome'],
                        r['curso'],
                        r['tipo'],
                        r['periodo'],
                        r['campus']
                    )
                    for r in records
                ]
                
                if batch_data:
                    from psycopg2.extras import execute_batch
                    # Note: execute_batch doesn't easily return row counts for ON CONFLICT DO NOTHING in generic driver
                    # So we might not get exact 'inserted' vs 'skipped' counts easily without RETURNING or checking before.
                    # For simplicity/performance in batch, we just execute.
                    # If we really need stats, execute_values with RETURNING is an option but more complex with ON CONFLICT.
                    # Let's assume all processed.
                    
                    execute_batch(cur, insert_query, batch_data)
                    conn.commit()
                    
                    # Since we can't easily count inserted vs skipped in simple batch without return,
                    # we'll report total extracted as processed. 
                    # If strict stats are needed, we would need a loop or more complex query.
                    inserted_count = len(batch_data) # This is technically "processed items count"
                
        print(f"Processed {len(records)} records for {faculdade} {ano}")
            
        stats = json.dumps({
            "extracted": len(records),
            "inserted": inserted_count, # Approx (includes skipped in this simple implementation)
            "skipped": 0, # Cannot track easily with batch insert + do nothing
            "failed": 0
        })

        # 5. Update status to completed
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