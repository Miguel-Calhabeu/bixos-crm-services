"""Parser for IFSP (São Carlos) SISU results."""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, asdict
from typing import List

import pdfplumber


HEADER_RE = re.compile(
    r"Campus\s+S[aã]o\s+Carlos\s+-\s+(?P<tipo>[^-]+?)\s+em\s+(?P<curso>.+?)\s+-\s+(?P<periodo>.+)$",
    re.IGNORECASE,
)
CANDIDATE_RE = re.compile(
    r"^\d{12}\s+(?P<nome>.+?)\s+\d{2}/\d{2}/\d{4}\s+\d",
)


@dataclass(frozen=True)
class Record:
    nome: str
    curso: str
    tipo: str
    periodo: str


def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\u00a0", " ")).strip()


def extract_records_from_bytes(file_bytes: bytes) -> List[dict]:
    curso = ""
    tipo = ""
    periodo = ""
    records: list[Record] = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw_line in text.splitlines():
                line = _clean_spaces(raw_line)
                if not line:
                    continue

                if not curso:
                    header_match = HEADER_RE.search(line)
                    if header_match:
                        tipo = _clean_spaces(header_match.group("tipo"))
                        curso = _clean_spaces(header_match.group("curso"))
                        periodo = _clean_spaces(header_match.group("periodo"))
                        continue

                candidate_match = CANDIDATE_RE.match(line)
                if candidate_match and curso:
                    nome = _clean_spaces(candidate_match.group("nome"))
                    records.append(Record(nome=nome, curso=curso, tipo=tipo, periodo=periodo))

    deduped: list[Record] = []
    seen: set[tuple[str, str]] = set()
    for r in records:
        key = (r.nome, r.curso)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    return [asdict(r) for r in deduped]
