from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import io
import json
try:
    # Deployment/runtime often imports this file as top-level `main.py`, with
    # `api/` as the working directory on sys.path.
    from parsers.dispatcher import extract_records_from_bytes_for_faculdade
except ModuleNotFoundError:
    # Local development may import as a package: `import api.main`.
    from api.parsers.dispatcher import extract_records_from_bytes_for_faculdade

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
SUPABASE_STORAGE_BASE_URL = os.environ.get("SUPABASE_STORAGE_BASE_URL")

processo_faculdade_map = {
    "ufscar": "UFSCar",
    "fuvest": "USP",
    "enem usp": "USP",
    "provao paulista (fuvest)": "USP",
    "fuvest lista de espera": "USP",
    "ifsp": "IFSP",
}

def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL environment variable not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _log(job_id: str, msg: str) -> None:
    # Keep logs single-line for Vercel readability.
    print(f"[{_now_ms()}][job:{job_id}] {msg}")


class _StepTimer:
    def __init__(self, job_id: str, step: str):
        self.job_id = job_id
        self.step = step
        self._t0 = time.perf_counter()

    def done(self, extra: str = "") -> None:
        dt_ms = (time.perf_counter() - self._t0) * 1000
        suffix = f" | {extra}" if extra else ""
        _log(self.job_id, f"{self.step} done in {dt_ms:.1f}ms{suffix}")

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

    _log(str(job_id), f"Processing job for faculdade={faculdade!r} ano={ano!r} url={storage_key}")

    try:
        # 1. Update status to parsing
        t = _StepTimer(str(job_id), "db:update_status_parsing")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE imports SET status = 'parsing', updated_at = NOW() WHERE id = %s", (job_id,))
                conn.commit()
        t.done()

        # 2. Download PDF
        t = _StepTimer(str(job_id), "http:download_pdf")
        # Avoid hanging network calls in serverless runtimes.
        # (connect timeout, read timeout)
        response = requests.get(storage_key, timeout=(10, 60))
        response.raise_for_status()
        pdf_bytes = response.content
        size = len(pdf_bytes)
        cl = response.headers.get("content-length")
        ct = response.headers.get("content-type")
        t.done(extra=f"status={response.status_code} bytes={size} content_length={cl} content_type={ct}")

        # 3. Parse PDF (select parser by faculdade)
        t = _StepTimer(str(job_id), "parse:extract_records")
        records = extract_records_from_bytes_for_faculdade(pdf_bytes, faculdade)
        t.done(extra=f"records={len(records)}")

        # 4. Insert directly into Database (leads_raw)
        inserted_count = 0
        skipped_count = 0

        t = _StepTimer(str(job_id), "db:insert_leads_raw")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                insert_query = """
                    INSERT INTO public.leads_raw (
                        "Faculdade", "Ano", "Nome", "Curso", "Tipo", "Periodo"
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT ("Faculdade", "Ano", "Nome") DO NOTHING
                """

                batch_data = [
                    (
                        processo_faculdade_map.get(faculdade.lower(), faculdade),
                        int(ano),
                        r['nome'],
                        r['curso'],
                        r['tipo'],
                        r['periodo']
                    )
                    for r in records
                ]

                if batch_data:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur, insert_query, batch_data)
                    conn.commit()
                    inserted_count = len(batch_data)
        t.done(extra=f"batch={inserted_count}")

        # 4b. Merge raw -> silver (keep the latest created_at per "Nome")
        # Note: This relies on Postgres MERGE (PG15+) support.
        t = _StepTimer(str(job_id), "db:merge_raw_to_silver")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    MERGE INTO public.leads_silver AS tgt
                    USING (
                      SELECT DISTINCT ON ("Nome")
                        "Nome",
                        "Ano",
                        "Faculdade",
                        "Curso",
                        "Tipo",
                        "Periodo",
                        created_at
                      FROM public.leads_raw
                      ORDER BY "Nome", created_at DESC
                    ) AS src
                    ON (tgt."Nome" = src."Nome")
                    WHEN MATCHED AND src.created_at > tgt.created_at THEN
                      UPDATE SET
                        "Ano" = src."Ano",
                        "Faculdade" = src."Faculdade",
                        "Curso" = src."Curso",
                        "Tipo" = src."Tipo",
                        "Periodo" = src."Periodo",
                        created_at = src.created_at,
                        updated_at = now()
                    WHEN NOT MATCHED THEN
                      INSERT (
                        "Nome",
                        "Ano",
                        "Faculdade",
                        "Curso",
                        "Tipo",
                        "Periodo",
                        created_at,
                        updated_at
                      )
                      VALUES (
                        src."Nome",
                        src."Ano",
                        src."Faculdade",
                        src."Curso",
                        src."Tipo",
                        src."Periodo",
                        src.created_at,
                        now()
                      );
                    """
                )
                conn.commit()
        t.done()

        # 4c. Upsert silver -> dimension_lead (seed CRM dimension)
        t = _StepTimer(str(job_id), "db:upsert_silver_to_dimension")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS dimension_lead_nome_uq
                      ON public.dimension_lead (nome);

                    INSERT INTO public.dimension_lead (
                      nome,
                                            responsavel_nome,
                      "Ano",
                      "Faculdade",
                      "Curso",
                      "Tipo",
                      "Periodo",
                      source_silver_created_at,
                      updated_at
                    )
                    SELECT
                      s."Nome" as nome,
                                            cr.responsavel_nome,
                      s."Ano",
                      s."Faculdade",
                      s."Curso",
                      s."Tipo",
                      s."Periodo",
                      s.created_at as source_silver_created_at,
                      now() as updated_at
                    FROM public.leads_silver s
                                        LEFT JOIN public.course_responsavel cr
                                            ON cr.curso = s."Curso"
                    ON CONFLICT (nome) DO UPDATE
                    SET
                                            responsavel_nome = COALESCE(public.dimension_lead.responsavel_nome, EXCLUDED.responsavel_nome),
                      "Ano" = EXCLUDED."Ano",
                      "Faculdade" = EXCLUDED."Faculdade",
                      "Curso" = EXCLUDED."Curso",
                      "Tipo" = EXCLUDED."Tipo",
                      "Periodo" = EXCLUDED."Periodo",
                      source_silver_created_at = EXCLUDED.source_silver_created_at,
                      updated_at = now()
                    WHERE
                      public.dimension_lead.source_silver_created_at IS NULL
                      OR EXCLUDED.source_silver_created_at > public.dimension_lead.source_silver_created_at;
                    """
                )
                conn.commit()
                t.done()

        # 4d. Seed initial CRM status for leads that don't have any events yet
        # Important: do NOT overwrite existing CRM history.
                t = _StepTimer(str(job_id), "db:seed_fact_crm")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.fact_crm (lead_id, status, observacoes, changed_by)
                    SELECT d.lead_id, 'Novo'::text AS status, NULL::text AS observacoes, 'pipeline'::text AS changed_by
                    FROM public.dimension_lead d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM public.fact_crm f WHERE f.lead_id = d.lead_id
                    );
                    """
                )
                conn.commit()
        t.done()

        _log(str(job_id), f"Processed records={len(records)} faculdade={faculdade!r} ano={ano!r}")

        stats = json.dumps({
            "extracted": len(records),
            "inserted": inserted_count, # Approx (includes skipped in this simple implementation)
            "skipped": 0, # Cannot track easily with batch insert + do nothing
            "failed": 0
        })

        # 5. Update status to completed
        t = _StepTimer(str(job_id), "db:update_status_completed")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE imports SET status = 'completed', stats_json = %s, updated_at = NOW() WHERE id = %s",
                    (stats, job_id)
                )
                conn.commit()
        t.done(extra=f"stats_json_len={len(stats)}")

        _log(str(job_id), "Job completed successfully")

    except Exception as e:
        _log(str(job_id), f"Job failed: {type(e).__name__}: {e}")
        try:
            t = _StepTimer(str(job_id), "db:update_status_failed")
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE imports SET status = 'failed', error = %s, updated_at = NOW() WHERE id = %s",
                        (str(e), job_id)
                    )
                    conn.commit()
            t.done()
        except Exception as db_e:
            _log(str(job_id), f"Failed to update error status: {type(db_e).__name__}: {db_e}")

def process_pending_jobs_task():
    print("Starting background job processing...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Fetch pending jobs
                cur.execute("SELECT * FROM imports WHERE status = 'queued_parse' OR status = 'pending' OR status = 'failed'")
                jobs = cur.fetchall()

        print(f"Found {len(jobs)} pending jobs")

        for job in jobs:
            process_job(job)

    except Exception as e:
        print(f"Error fetching jobs: {e}")

@app.get("/")
def read_root():
    return {"status": "ok", "message": "PDF Parser API is running"}

@app.post("/process-jobs")
async def trigger_job_processing(background_tasks: BackgroundTasks):
    """
    Endpoint triggered by Next.js to start processing pending jobs.
    """
    background_tasks.add_task(process_pending_jobs_task)
    return {"status": "processing_started", "message": "Background task initiated"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
