"""Parser for Provão Paulista PDF (Lista de Espera)."""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, asdict
from typing import List

import pdfplumber


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
    return re.sub(r"\s+", " ", s.replace("\u00a0", " ")).strip()


def _is_sao_carlos_campus(raw: str) -> bool:
    c = _clean_spaces(raw).lower()
    return "sao carlos" in c or "são carlos" in c


def extract_records_from_bytes(file_bytes: bytes) -> List[dict]:
    records: list[Record] = []
    current_name: str | None = None
    current_course: tuple[str, str, str] | None = None

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
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
