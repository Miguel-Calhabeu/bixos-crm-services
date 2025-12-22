"""Smoke test for GOLD/CRM layer.

What it checks:
1) Creates a synthetic lead in leads_silver
2) Runs the upsert silver->dimension_lead
3) Inserts a fact_crm event (status)
4) Inserts a fact_visit event (timestamp)
5) Queries back and asserts everything is linked

Requires DATABASE_URL.

Notes:
- This script assumes the migrations for dimension_lead/fact_crm/fact_visit were applied.
- It won't run migrations automatically; it's just a sanity check.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping, cast

import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.environ.get("DATABASE_URL")


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


UPSERT_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS dimension_lead_nome_uq
  ON public.dimension_lead (nome);

INSERT INTO public.dimension_lead (
  nome,
  "Ano",
  "Faculdade",
  "Curso",
  "Tipo",
  "Periodo",
  "Campus",
  source_silver_created_at,
  updated_at
)
SELECT
  s."Nome" as nome,
  s."Ano",
  s."Faculdade",
  s."Curso",
  s."Tipo",
  s."Periodo",
  s."Campus",
  s.created_at as source_silver_created_at,
  now() as updated_at
FROM public.leads_silver s
ON CONFLICT (nome) DO UPDATE
SET
  "Ano" = EXCLUDED."Ano",
  "Faculdade" = EXCLUDED."Faculdade",
  "Curso" = EXCLUDED."Curso",
  "Tipo" = EXCLUDED."Tipo",
  "Periodo" = EXCLUDED."Periodo",
  "Campus" = EXCLUDED."Campus",
  source_silver_created_at = EXCLUDED.source_silver_created_at,
  updated_at = now()
WHERE
  public.dimension_lead.source_silver_created_at IS NULL
  OR EXCLUDED.source_silver_created_at > public.dimension_lead.source_silver_created_at;
"""


def main() -> None:
    nome = f"CRM_SMOKE_{uuid.uuid4().hex[:10]}"
    now_utc = datetime.now(timezone.utc)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Seed a silver row
            cur.execute(
                """
                INSERT INTO public.leads_silver (
                  "Nome","Ano","Faculdade","Curso","Tipo","Periodo","Campus", created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now())
                ON CONFLICT ("Nome") DO UPDATE
                SET created_at = EXCLUDED.created_at,
                    updated_at = now();
                """,
                (nome, 2025, "UFSCar", "Direito", "AC", "Integral", "Sao Carlos", now_utc),
            )

            # Upsert into dimension
            cur.execute(UPSERT_SQL)

            # Get lead_id
            cur.execute("SELECT lead_id FROM public.dimension_lead WHERE nome = %s", (nome,))
            row = cur.fetchone()
            if not row:
                raise AssertionError("dimension_lead row not created")
            lead_id = cast(Mapping[str, Any], row)["lead_id"]

            # Insert CRM event
            cur.execute(
                """
                INSERT INTO public.fact_crm (lead_id, status, observacoes, changed_at)
                VALUES (%s,%s,%s,%s)
                """,
                (lead_id, "novo", "primeiro contato pendente", now_utc),
            )

            # Insert Visit
            cur.execute(
                """
                INSERT INTO public.fact_visit (lead_id, visited_at, source, notes)
                VALUES (%s,%s,%s,%s)
                """,
                (lead_id, now_utc, "instagram", "visitou o perfil"),
            )

            # Validate
            cur.execute(
                """
                SELECT
                  d.nome,
                  d."Faculdade",
                  d."Curso",
                  (
                    SELECT f.status
                    FROM public.fact_crm f
                    WHERE f.lead_id = d.lead_id
                    ORDER BY f.changed_at DESC
                    LIMIT 1
                  ) AS current_status,
                  (
                    SELECT v.visited_at
                    FROM public.fact_visit v
                    WHERE v.lead_id = d.lead_id
                    ORDER BY v.visited_at DESC
                    LIMIT 1
                  ) AS last_visit
                FROM public.dimension_lead d
                WHERE d.lead_id = %s
                """,
                (lead_id,),
            )
            out = cur.fetchone()
            if not out:
                raise AssertionError("Failed to query joined CRM view")
            out_m = cast(Mapping[str, Any], out)

            assert out_m["nome"] == nome
            assert out_m["current_status"] == "novo"
            assert out_m["last_visit"] is not None

        conn.commit()

    print("OK: CRM GOLD layer smoke test passed")


if __name__ == "__main__":
    main()
