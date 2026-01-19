"""Parser for Provão Paulista PDF.

The Provão Paulista PDFs have changed formats over time.

Contract (same as other parsers):
- Input: raw PDF bytes
- Output: list[dict] with keys: nome, curso, tipo, periodo

We only ingest candidates for São Carlos campuses.
"""

from __future__ import annotations

import io
import re
import unicodedata
from dataclasses import dataclass, asdict
from typing import Any, List

import pdfplumber


# --- New format (2025+) ---
# Example (may wrap across lines):
#   ADRIELE CRISTINE CHAVES GONCALVES ***994.898** B USP - 90011/104 - Ciências Exatas
#   (Licenciatura) - Noturno - Instituto de Física de São Carlos
# There are also other institutions (FATEC/UNESP/UNICAMP). We only keep rows
# where the full course/location text contains "São Carlos".

MASKED_ID_RE = re.compile(r"\*{3}\d{1,3}[\.,]\d{3}\*{2}")

# Split between name and the rest: <NOME> <masked id> <grupo> <curso...>
NEW_ROW_RE = re.compile(
    r"^(?P<nome>.+?)\s+(?P<id>\*{3}\d{1,3}[\.,]\d{3}\*{2})\s+(?P<grupo>[A-Z])\s+(?P<curso_blob>.+)$"
)

# Old format (kept for backward compatibility)
NAME_LINE_RE = re.compile(r"^\d{1,3}\.\d{3}\s+(?P<nome>.+)$")
COURSE_LINE_RE = re.compile(
    r"^\d+(?:/[\dA-Za-z]+)?\s+(?P<curso>.+?)\s*\((?P<tipo>[^)]+)\)\s*-\s*(?P<periodo>.+)$"
)
POSITION_RE = re.compile(r"^\d+/\d+$")


@dataclass(frozen=True)
class Record:
    nome: str
    curso: str
    tipo: str
    periodo: str


def _clean_spaces(s: str) -> str:
    # Also normalize line-wrapped words that pdfplumber may emit with newlines.
    s = s.replace("\u00a0", " ").replace("\n", " ")
    return re.sub(r"\s+", " ", s).strip()


def _is_sao_carlos_campus(raw: str) -> bool:
    c = _clean_spaces(raw).lower()
    c = unicodedata.normalize("NFKD", c)
    c = "".join(ch for ch in c if not unicodedata.combining(ch))
    return "sao carlos" in c


def _is_usp_institution(course_blob: str) -> bool:
        """Return True if the course blob belongs to USP.

        New-format blobs begin with an institution segment like:
            "USP - 90011/104 - ..."
        """

        first = _clean_spaces(course_blob.split(" - ", 1)[0]).upper()
        return first == "USP"


def _split_course_blob(course_blob: str) -> tuple[str, str, str]:
    """Parse '<instituição> - <código> - <curso> - <periodo> - <local>' blobs.

    Returns (curso, tipo, periodo) following the shared ingestion contract.

    Examples (with line wraps removed):
    - "USP - 86300/103 - Gerontologia (Bacharelado) - Vespertino - São Paulo (...)"
    - "USP - 90011/104 - Ciências Exatas (Licenciatura) - Noturno - Instituto de Física de São Carlos"

    Notes:
    - The first segment is the institution label (USP/UNESP/FATEC/UNICAMP) and is
      *not* our `tipo` field.
    - `tipo` is usually inside the course segment as a trailing "(...)".
    """

    parts = [_clean_spaces(p) for p in course_blob.split(" - ") if _clean_spaces(p)]
    if not parts:
        return "", "", ""

    def _extract_tipo_from_curso(curso_text: str) -> tuple[str, str]:
        m = re.search(r"\((?P<tipo>[^)]+)\)\s*$", curso_text)
        if not m:
            return _clean_spaces(curso_text), ""
        tipo_local = _clean_spaces(m.group("tipo"))
        curso_clean = _clean_spaces(re.sub(r"\s*\([^)]+\)\s*$", "", curso_text))
        return curso_clean, tipo_local

    # Typical layout: INSTITUICAO - CODIGO - CURSO - PERIODO - LOCAL
    if len(parts) >= 4:
        periodo = parts[-2]
        curso_raw = " - ".join(parts[2:-2]) if len(parts) > 4 else parts[2]
        curso, tipo = _extract_tipo_from_curso(curso_raw)
        return curso, tipo, periodo

    # Fallback: INSTITUICAO - CURSO - PERIODO
    if len(parts) == 3:
        curso, tipo = _extract_tipo_from_curso(parts[1])
        return curso, tipo, parts[2]

    # Last resort: treat remaining as course text.
    curso, tipo = _extract_tipo_from_curso(" - ".join(parts[1:]))
    return curso, tipo, ""


def _iter_new_format_rows(pdf: Any) -> List[str]:
    """Return a list of reconstructed logical rows for the new table format.

    pdfplumber sometimes wraps long rows into multiple lines. We rebuild a row
    by accumulating text until we have seen the masked id and the group token.
    """

    rows: list[str] = []
    buffer: list[str] = []

    def flush_if_row() -> None:
        if not buffer:
            return
        candidate = _clean_spaces(" ".join(buffer))
        if NEW_ROW_RE.match(candidate):
            rows.append(candidate)
            buffer.clear()

    for page in pdf.pages:
        # layout=False tends to avoid inserting awkward line breaks inside words.
        text = page.extract_text(layout=False) or ""
        for raw_line in text.splitlines():
            line = _clean_spaces(raw_line)
            if not line:
                continue

            # Skip headers/footers for the new PDF.
            if line.startswith("Provão Paulista"):
                continue
            if line.startswith("Lista de convocação"):
                continue
            if line.lower().startswith("nome do candidato"):
                continue

            # Many lines are actually multiple columns merged; treat every line
            # as a continuation unless it's the start of a new row.
            if MASKED_ID_RE.search(line):
                # Likely start (or middle) of a row.
                buffer.append(line)
                flush_if_row()
                continue

            if buffer:
                buffer.append(line)
                flush_if_row()

    # Best-effort flush at end
    if buffer:
        candidate = _clean_spaces(" ".join(buffer))
        if NEW_ROW_RE.match(candidate):
            rows.append(candidate)

    return rows


def extract_records_from_bytes(file_bytes: bytes) -> List[dict]:
    records: list[Record] = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        # Try new format first.
        for row in _iter_new_format_rows(pdf):
            m = NEW_ROW_RE.match(row)
            if not m:
                continue

            nome = _clean_spaces(m.group("nome")).replace("*", "").strip()
            course_blob = _clean_spaces(m.group("curso_blob"))

            # Filter: only USP entries for São Carlos.
            if not _is_usp_institution(course_blob):
                continue
            if not _is_sao_carlos_campus(course_blob):
                continue

            curso, tipo, periodo = _split_course_blob(course_blob)
            if not (nome and curso):
                continue
            records.append(Record(nome=nome, curso=curso, tipo=tipo, periodo=periodo))

        # Backwards compatible parsing: old "Lista de Espera" layout.
        if not records:
            current_name: str | None = None
            current_course: tuple[str, str, str] | None = None

            for page in pdf.pages:
                text = page.extract_text() or ""
                for raw_line in text.splitlines():
                    line = _clean_spaces(raw_line)
                    if not line:
                        continue

                    if POSITION_RE.match(line):
                        continue
                    if line.startswith("Provão Paulista"):
                        continue
                    if "Lista de Espera" in line:
                        continue
                    if line.startswith("Processamento"):
                        continue

                    name_match = NAME_LINE_RE.match(line)
                    if name_match:
                        current_name = _clean_spaces(name_match.group("nome"))
                        continue

                    course_match = COURSE_LINE_RE.match(line)
                    if course_match:
                        current_course = (
                            _clean_spaces(course_match.group("curso")),
                            _clean_spaces(course_match.group("tipo")),
                            _clean_spaces(course_match.group("periodo")),
                        )
                        continue

                    if line.lower().startswith("campus"):
                        if current_name and current_course and _is_sao_carlos_campus(line):
                            curso, tipo, periodo = current_course
                            records.append(
                                Record(
                                    nome=current_name,
                                    curso=curso,
                                    tipo=tipo,
                                    periodo=periodo,
                                )
                            )
                        current_name = None
                        current_course = None
                        continue

    deduped: list[Record] = []
    seen: set[tuple[str, str]] = set()
    for r in records:
        key = (r.nome, r.curso)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    return [asdict(r) for r in deduped]

if __name__ == "__main__":
    import sys
    from pprint import pprint

    with open(sys.argv[1], "rb") as f:
        data = f.read()
    records = extract_records_from_bytes(data)
    pprint(records)
