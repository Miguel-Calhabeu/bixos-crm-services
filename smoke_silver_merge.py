"""Smoke test for silver merge logic.

This is intentionally a tiny script (not a full test suite) because the repo
currently doesn't have a Python test runner wired.

It:
- creates a unique Nome
- inserts two rows into public.leads_raw with different created_at
- runs the same MERGE used by the API
- asserts public.leads_silver keeps the newest created_at row

Requires DATABASE_URL env var.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, cast

import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.environ.get("DATABASE_URL")


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


MERGE_SQL = """
MERGE INTO public.leads_silver AS tgt
USING (
  SELECT DISTINCT ON (\"Nome\")
    \"Nome\",
    \"Ano\",
    \"Faculdade\",
    \"Curso\",
    \"Tipo\",
    \"Periodo\",
    \"Campus\",
    created_at
  FROM public.leads_raw
  ORDER BY \"Nome\", created_at DESC
) AS src
ON (tgt.\"Nome\" = src.\"Nome\")
WHEN MATCHED AND src.created_at > tgt.created_at THEN
  UPDATE SET
    \"Ano\" = src.\"Ano\",
    \"Faculdade\" = src.\"Faculdade\",
    \"Curso\" = src.\"Curso\",
    \"Tipo\" = src.\"Tipo\",
    \"Periodo\" = src.\"Periodo\",
    \"Campus\" = src.\"Campus\",
    created_at = src.created_at,
    updated_at = now()
WHEN NOT MATCHED THEN
  INSERT (
    \"Nome\",
    \"Ano\",
    \"Faculdade\",
    \"Curso\",
    \"Tipo\",
    \"Periodo\",
    \"Campus\",
    created_at,
    updated_at
  )
  VALUES (
    src.\"Nome\",
    src.\"Ano\",
    src.\"Faculdade\",
    src.\"Curso\",
    src.\"Tipo\",
    src.\"Periodo\",
    src.\"Campus\",
    src.created_at,
    now()
  );
"""


def main() -> None:
    nome = f"SMOKE_{uuid.uuid4().hex[:10]}"
    t1 = datetime.now(timezone.utc) - timedelta(days=1)
    t2 = datetime.now(timezone.utc)

    older = {
        "Faculdade": "UFSCar",
        "Ano": 2025,
        "Nome": nome,
        "Curso": "Engenharia",
        "Tipo": "AC",
        "Periodo": "Integral",
        "Campus": "Sao Carlos",
        "created_at": t1,
    }
    newer = {
        **older,
        "Curso": "Ciencia da Computacao",
        "created_at": t2,
    }

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Ensure target table exists (migration should handle it, but this is friendly)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.leads_silver (
                  \"Nome\" text PRIMARY KEY,
                  \"Ano\" integer NOT NULL,
                  \"Faculdade\" text NOT NULL,
                  \"Curso\" text NOT NULL,
                  \"Tipo\" text NOT NULL,
                  \"Periodo\" text NOT NULL,
                  \"Campus\" text NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                );
                """
            )

            # Insert both versions into raw (bypass ON CONFLICT created_at limitation by using different PKs)
            # leads_raw PK is (Faculdade, Ano, Nome), so we need different Faculdade/Ano to store 2 rows.
            # But the business rule for silver is per Nome only.
            cur.execute(
                """
                INSERT INTO public.leads_raw (
                  \"Faculdade\", \"Ano\", \"Nome\", \"Curso\", \"Tipo\", \"Periodo\", \"Campus\", created_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (\"Faculdade\", \"Ano\", \"Nome\") DO NOTHING
                """,
                (
                    older["Faculdade"],
                    older["Ano"],
                    older["Nome"],
                    older["Curso"],
                    older["Tipo"],
                    older["Periodo"],
                    older["Campus"],
                    older["created_at"],
                ),
            )
            cur.execute(
                """
                INSERT INTO public.leads_raw (
                  \"Faculdade\", \"Ano\", \"Nome\", \"Curso\", \"Tipo\", \"Periodo\", \"Campus\", created_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (\"Faculdade\", \"Ano\", \"Nome\") DO NOTHING
                """,
                (
                    newer["Faculdade"] + "_2",
                    newer["Ano"] + 1,
                    newer["Nome"],
                    newer["Curso"],
                    newer["Tipo"],
                    newer["Periodo"],
                    newer["Campus"],
                    newer["created_at"],
                ),
            )

            # Run merge
            cur.execute(MERGE_SQL)

            # Assert
            cur.execute(
                "SELECT \"Curso\", created_at FROM public.leads_silver WHERE \"Nome\" = %s",
                (nome,),
            )
            row = cur.fetchone()
            if not row:
                raise AssertionError("No row found in leads_silver")

            row_m = cast(Mapping[str, Any], row)

            if row_m["Curso"] != newer["Curso"]:
                raise AssertionError(f"Expected Curso='{newer['Curso']}', got '{row_m['Curso']}'")

            # created_at roundtrip: compare as ISO strings without nanos if needed
            got_created_at = row_m["created_at"]
            if isinstance(got_created_at, str):
                got_dt = datetime.fromisoformat(got_created_at.replace("Z", "+00:00"))
            else:
                got_dt = got_created_at

            if got_dt.replace(microsecond=0) != t2.replace(microsecond=0):
                raise AssertionError(f"Expected created_at around {t2}, got {got_dt}")

        conn.commit()

    print("OK: leads_silver kept the latest created_at per Nome")


if __name__ == "__main__":
    main()
