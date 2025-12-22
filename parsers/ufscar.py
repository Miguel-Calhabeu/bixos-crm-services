"""UFSCar PDF parser.

We still *extract* the campus portion from the course header line, but the
pipeline no longer stores campus in the DB.

Instead, we use campus only for filtering: we ingest only rows for
**Campus São Carlos**.
"""

import re
from dataclasses import dataclass, asdict
from typing import Optional, List
import pdfplumber
import io

COURSE_LINE_RE = re.compile(
    r"^.+\s-\s(?:Bacharelado|Licenciatura|Tecn[oó]logo|Engenharia|Medicina|Administra[cç][aã]o|\w+)\s-\s.+$",
    re.IGNORECASE,
)

ROW_RE = re.compile(
    r"^\s*\d{2}\*{2,}\d+\s+"  # masked ENEM inscription
    r"(?P<nome>.+?)\s+"  # name
    r"(?P<grupo>[A-Z]{1,3}(?:_[A-Z]{2,5})*)\s+"  # group
    r"(?P<nota>\d{1,3}(?:[\.,]\d{2})?)\s*$",  # score
)

FOOTER_RE = re.compile(r"^Emitido em:|^P[áa]gina\s+\d+\s+de\s+\d+", re.IGNORECASE)
HEADER_SKIP_RE = re.compile(
    r"^(Convoca[cç][aã]o|Processo Seletivo|UFSCar|\d+ª\s+Chamada|Insc\.\s+Enem\s+Nome\s+do\s+Candidato)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Record:
    nome: str
    curso: str
    tipo: str
    periodo: str


def split_course_section(course_section: str) -> tuple[str, str, str, str]:
    """Split course header line into (curso, tipo, periodo, campus).

    Some PDFs include campus as the 4th+ segment. We keep it only so we can
    filter to São Carlos.
    """

    parts = [_clean_spaces(p) for p in course_section.split(" - ")]
    if len(parts) >= 4:
        curso = parts[0]
        tipo = parts[1]
        periodo = parts[2]
        campus = " - ".join(parts[3:])
        return curso, tipo, periodo, campus

    if len(parts) >= 3:
        curso = parts[0]
        tipo = parts[1]
        periodo = parts[2]
        return curso, tipo, periodo, ""

    return _clean_spaces(course_section), "", "", ""


def _is_sao_carlos_campus(campus: str) -> bool:
    # Be tolerant to capitalization/accents and allow strings like:
    # - "Campus São Carlos"
    # - "CAMPUS SAO CARLOS"
    c = _clean_spaces(campus).lower()
    return "são carlos" in c or "sao carlos" in c


def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\u00a0", " ")).strip()


def extract_records_from_bytes(file_bytes: bytes) -> List[dict]:
    records: list[Record] = []
    current_course: Optional[str] = None

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw_line in text.splitlines():
                line = _clean_spaces(raw_line)
                if not line:
                    continue

                if FOOTER_RE.search(line):
                    continue
                if HEADER_SKIP_RE.search(line):
                    continue

                if COURSE_LINE_RE.match(line) and "Nome do Candidato" not in line:
                    current_course = line
                    continue

                m = ROW_RE.match(line)
                if m and current_course:
                    nome = _clean_spaces(m.group("nome"))
                    curso, tipo, periodo, campus = split_course_section(current_course)
                    if campus and not _is_sao_carlos_campus(campus):
                        continue
                    records.append(Record(nome=nome, curso=curso, tipo=tipo, periodo=periodo))

    seen = set()
    deduped = []
    for r in records:
        key = (r.nome, r.curso)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(asdict(r))

    return deduped
