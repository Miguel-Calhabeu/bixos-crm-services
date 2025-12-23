"""Parser for ENEM USP call list (1ª matrícula)."""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, asdict
from typing import List

import pdfplumber


COURSE_RE = re.compile(
    r"^(?P<curso>.+?)\s+[\-−]\s+\((?P<tipo>[^)]+)\)\s+[\-−]\s+(?P<periodo>.+)$"
)
CANDIDATE_RE = re.compile(r"^\d+\s+\d{3}\.\d{3}\s+(?P<nome>.+)$")
HEADER_SKIP_RE = re.compile(
    r"^(ENEM\s+USP\s+\d+|CHAMADOS\s+PARA\s+A\s+PRIMEIRA\s+MATR[IÍ]CULA|PROCESSAMENTO\s+REALIZADO)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Record:
    nome: str
    curso: str
    tipo: str
    periodo: str


def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\u00a0", " ")).strip()


def _is_sao_carlos_campus(text: str) -> bool:
    c = _clean_spaces(text).lower()
    return "sao carlos" in c or "são carlos" in c


def _looks_like_institution_line(text: str) -> bool:
    return "usp" in text.lower()


def extract_records_from_bytes(file_bytes: bytes) -> List[dict]:
    records: list[Record] = []
    current_course: tuple[str, str, str] | None = None
    current_institution: str = ""

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text(layout=True) or ""
            for raw_line in text.splitlines():
                line = _clean_spaces(raw_line)
                if not line:
                    continue
                if HEADER_SKIP_RE.match(line):
                    continue

                course_match = COURSE_RE.match(line)
                if course_match:
                    current_course = (
                        _clean_spaces(course_match.group("curso")),
                        _clean_spaces(course_match.group("tipo")),
                        _clean_spaces(course_match.group("periodo")),
                    )
                    current_institution = ""
                    continue

                if _looks_like_institution_line(line):
                    current_institution = line
                    continue

                candidate_match = CANDIDATE_RE.match(line)
                if candidate_match and current_course:
                    if _is_sao_carlos_campus(current_institution):
                        curso, tipo, periodo = current_course
                        nome = _clean_spaces(candidate_match.group("nome"))
                        records.append(Record(nome=nome, curso=curso, tipo=tipo, periodo=periodo))
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

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <enem-usp-pdf-file>", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1], "rb") as f:
        data = f.read()

    results = extract_records_from_bytes(data)
    pprint(results)
