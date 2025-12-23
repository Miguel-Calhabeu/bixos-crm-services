"""Fuvest PDF parser for "1ª Chamada" list.

Extracts candidate names and maps the carreira−curso code to the
standard course/tipo/período columns using ``codigo-dimension.csv``.
Only candidates whose code exists in ``codigo-dimension.csv`` are
returned (inner join to Campus São Carlos courses).
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple

import pdfplumber

CPF_RE = re.compile(r"^\d{3}\.\d{3}$")
CODE_FULL_RE = re.compile(r"^\d{3}[\-−]\d{2}$")


@dataclass(frozen=True)
class Record:
    nome: str
    curso: str
    tipo: str
    periodo: str


@dataclass(frozen=True)
class DimensionRow:
    curso: str
    tipo: str
    periodo: str


_DIMENSION_CACHE: Dict[str, DimensionRow] | None = None


def _load_dimension_map() -> Dict[str, DimensionRow]:
    global _DIMENSION_CACHE
    if _DIMENSION_CACHE is not None:
        return _DIMENSION_CACHE

    path = Path(__file__).with_name("codigo-dimension.csv")
    mapping: Dict[str, DimensionRow] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            codigo = row.get("CODIGO", "").strip()
            if not codigo:
                continue
            mapping[codigo] = DimensionRow(
                curso=row.get("CURSO", "").strip(),
                tipo=row.get("TIPO", "").strip(),
                periodo=row.get("PERIODO", "").strip(),
            )

    _DIMENSION_CACHE = mapping
    return mapping


def _clean_name(raw: str) -> str:
    cleaned = re.sub(r"\s+", " ", raw.replace("\u00a0", " ")).strip()
    return cleaned.replace("...", "")


def _normalize_code(raw: str) -> str | None:
    code = raw.replace("−", "-").strip().strip(".,; ")
    if CODE_FULL_RE.match(code):
        return code
    return None


def _consume_code(tokens: List[str], start: int) -> Tuple[str | None, int]:
    """Return (normalized_code, consumed_tokens_after_start)."""

    if start >= len(tokens):
        return None, 0

    first = tokens[start]
    consumed = 1

    # Handle splits like "106" "−13" or "513−2" "5" or "313−" "54".
    pieces = [first]
    next_token = tokens[start + 1] if start + 1 < len(tokens) else ""

    if ("−" not in first and "-" not in first) and next_token.startswith("−"):
        pieces.append(next_token)
        consumed = 2
    elif re.match(r"^\d{3}[\-−]\d$", first) and next_token.isdigit():
        pieces.append(next_token)
        consumed = 2
    elif re.match(r"^\d{3}[\-−]$", first) and next_token.isdigit():
        pieces.append(next_token)
        consumed = 2
    elif CODE_FULL_RE.match(first):
        pass

    raw_code = "".join(pieces)
    return _normalize_code(raw_code), consumed


def extract_records_from_bytes(file_bytes: bytes) -> List[dict]:
    dimension = _load_dimension_map()
    records: List[Record] = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        tokens: List[str] = []
        for page in pdf.pages:
            text = page.extract_text(layout=False) or ""
            for line in text.split("\n"):
                # Skip obvious headers.
                if line.strip().upper().startswith("NOME CPF CURSO"):
                    continue
                tokens.extend(line.split())

    name_tokens: List[str] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if CPF_RE.match(tok):
            nome = _clean_name(" ".join(name_tokens))
            code, consumed = _consume_code(tokens, i + 1)
            i += consumed
            if nome and code and code in dimension:
                dim = dimension[code]
                records.append(
                    Record(
                        nome=nome,
                        curso=dim.curso,
                        tipo=dim.tipo,
                        periodo=dim.periodo,
                    )
                )
            name_tokens = []
        else:
            name_tokens.append(tok)
        i += 1

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

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <fuvest-pdf-file>", file=sys.stderr)
        sys.exit(1)

    pdf_path = Path(sys.argv[1])
    with pdf_path.open("rb") as f:
        file_bytes = f.read()

    records = extract_records_from_bytes(file_bytes)
    for record in records:
        print(record)
