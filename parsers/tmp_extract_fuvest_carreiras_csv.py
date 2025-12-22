"""Temporary script to extract carreira-curso codes and campus from
`fuvest2025_guia-carreiras.pdf`.

Output CSV columns:
- CODIGO: "{carreira}-{curso}" (e.g., "109-21")
- CAMPUS: campus name as in the PDF (e.g., "Ribeirão Preto")

This script is intentionally standalone and tolerant to small layout quirks.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pdfplumber

PDF_PATH = Path(__file__).with_name("fuvest2025_guia-carreiras.pdf")
OUT_CSV_PATH = Path(__file__).with_name("fuvest2025_guia-carreiras_codigo_campus.csv")


@dataclass(frozen=True)
class Row:
    carreira: str
    curso: str
    curso_nome: str
    tipo: str  # raw: B or L
    periodo: str
    campus: str
    page: int  # 1-indexed page number in the PDF

    @property
    def codigo(self) -> str:
        return f"{self.carreira}-{self.curso}"


def _tipo_label(raw: str) -> str:
    raw = raw.strip().upper()
    if raw == "B":
        return "Bacharelado"
    if raw == "L":
        return "Licenciatura"
    if raw in {"B/L", "L/B"}:
        return "Bacharelado/Licenciatura"
    return raw


def _cleanup_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _parse_row_words(
    row_words: List[dict],
    last_carreira: Optional[str],
    page_number: int,
) -> Tuple[Optional[Row], Optional[str]]:
    """Parse one visual table row from word dicts (pdfplumber.extract_words).

    We rely on column x positions, which are stable across pages:
    - CARREIRA: far-left
    - CURSO: next numeric column
    - CURSO (name): text column
    - TIPO: a small column with B or L (sometimes missing in extraction)
    - PERIODO: Integral/Matutino/Vespertino/Noturno
    - UNIDADE: short code (ignored)
    - CAMPUS: text column

    Then vacancy numbers start; we stop before the first vacancy number.
    """

    row_words = sorted(row_words, key=lambda w: w["x0"])
    if not row_words:
        return None, last_carreira

    # We key off the stable x anchors observed in this PDF:
    # - optional CARREIRA number at x~40
    # - CURSO number at x~170
    # - TIPO at x~306 (some pages x~354)
    # - PERIODO starts right after TIPO
    # - UNIDADE at x~386..432
    # - CAMPUS at x~450..496
    X_CARREIRA_COL_MAX = 120
    X_CURSO_COL_MIN, X_CURSO_COL_MAX = 140, 220
    # We'll locate column anchors (TIPO, UNIDADE, first VAGAS number) dynamically,
    # then slice the row by x positions.
    X_VAGAS_COL_MIN = 545

    def in_range(w, a, b):
        return a <= float(w["x0"]) < b

    # Identify curso number: the leftmost numeric token in the curso column band.
    curso_candidates = [w for w in row_words if w["text"].isdigit() and in_range(w, X_CURSO_COL_MIN, X_CURSO_COL_MAX)]
    if not curso_candidates:
        # Might be a carreira header row like "112 Medicina Veterinária".
        carreira_candidates = [w for w in row_words if w["text"].isdigit() and float(w["x0"]) < X_CARREIRA_COL_MAX]
        if carreira_candidates:
            return None, carreira_candidates[0]["text"]
        return None, last_carreira

    curso = min(curso_candidates, key=lambda w: float(w["x0"]))["text"]

    # Identify carreira number: numeric token at far-left.
    # Only trust it as CARREIRA when there is also a curso-column number in this row.
    carreira_candidates = [w for w in row_words if w["text"].isdigit() and float(w["x0"]) < X_CARREIRA_COL_MAX]
    carreira = carreira_candidates[0]["text"] if carreira_candidates else last_carreira
    if carreira is None:
        # If no explicit carreira yet, we can't build CODIGO.
        return None, last_carreira

    # Find TIPO anchor as the first B/L/BB token after the course-name area.
    tipo_word = next(
        (
            w
            for w in row_words
            if w["text"] in {"B", "L", "BB"} and float(w["x0"]) >= 260
        ),
        None,
    )
    if tipo_word is None:
        return None, carreira
    tipo_x0 = float(tipo_word["x0"])
    tipo_raw = "B" if tipo_word["text"] == "BB" else tipo_word["text"]

    # Find UNIDADE anchor: first token after TIPO with x0>=370 and before vagas.
    unidade_word = next(
        (
            w
            for w in row_words
            if float(w["x0"]) >= 370
            and float(w["x0"]) < X_VAGAS_COL_MIN
            and w["text"] not in {"!"}
            and not w["text"].isdigit()
        ),
        None,
    )
    if unidade_word is None:
        return None, carreira
    unidade_x0 = float(unidade_word["x0"])

    # Curso nome: tokens between curso number column and TIPO column.
    curso_nome_tokens = [
        w["text"]
        for w in row_words
        if float(w["x0"]) >= X_CURSO_COL_MIN
        and float(w["x0"]) < tipo_x0 - 1
        and not w["text"].isdigit()
        and w["text"] not in {";", "B", "L", "BB"}
    ]
    curso_nome = _cleanup_spaces(" ".join(curso_nome_tokens))

    # Período: tokens between TIPO and UNIDADE.
    periodo_tokens = [
        w["text"]
        for w in row_words
        if float(w["x0"]) > tipo_x0
        and float(w["x0"]) < unidade_x0 - 1
        and w["text"] not in {"!"}
    ]
    periodo = _cleanup_spaces(" ".join(periodo_tokens))
    periodo = re.sub(r"^(B|L)\s+", "", periodo).strip()
    if not periodo:
        return None, carreira

    # Campus: tokens after UNIDADE until the first vacancy number area.
    campus_text_parts: List[str] = []
    for w in row_words:
        if float(w["x0"]) < unidade_x0 + 1:
            continue
        if float(w["x0"]) >= X_VAGAS_COL_MIN:
            break
        t = w["text"]
        if t in {"!", "!!"}:
            continue
        if t.isdigit():
            break
        campus_text_parts.append(t)
    campus = _cleanup_spaces(" ".join(campus_text_parts))
    if not campus:
        return None, carreira

    return (
        Row(
            carreira=str(carreira),
            curso=str(curso),
            curso_nome=curso_nome,
            tipo=tipo_raw,
            periodo=periodo,
            campus=campus,
            page=page_number,
        ),
        str(carreira),
    )


def extract_rows(pdf_path: Path) -> List[Row]:
    rows: List[Row] = []

    def iter_row_words(page) -> Iterable[List[dict]]:
        # Word-based extraction is more reliable for table-like PDFs.
        words = page.extract_words(use_text_flow=True) or []
        if not words:
            return

        # Group words by visual row using y coordinate.
        y_tol = 2.5
        words_sorted = sorted(words, key=lambda w: (w["top"], w["x0"]))
        current: List[dict] = []
        current_top: Optional[float] = None

        for w in words_sorted:
            top = float(w["top"])
            if current_top is None or abs(top - current_top) <= y_tol:
                current.append(w)
                current_top = top if current_top is None else (current_top + top) / 2
            else:
                yield current
                current = [w]
                current_top = top

        if current:
            yield current

    with pdfplumber.open(pdf_path) as pdf:
        # Requested range: pages 8..14 (1-indexed) inclusive => indices 7..13.
        last_carreira: Optional[str] = None
        pending_course_name: Optional[str] = None
        for page_index, page in enumerate(pdf.pages[7:14], start=8):
            pending_course_name = None
            for rw in iter_row_words(page):
                rw_sorted = sorted(rw, key=lambda w: w["x0"])
                has_curso_num = any(ww["text"].isdigit() and 150 <= ww["x0"] < 240 for ww in rw_sorted)

                # If this row doesn't have a course number, it might be a continuation line
                # for a long course name (common in pages with Engineering options).
                if not has_curso_num:
                    txt = " ".join(ww["text"] for ww in rw_sorted if ww["text"] not in {"!", "!!"})
                    txt = _cleanup_spaces(txt)
                    # Ignore obvious non-name lines.
                    if txt and not txt.startswith("FUVEST -") and txt not in {"Início", "Clique para informações"}:
                        pending_course_name = txt
                    continue

                r, last_carreira = _parse_row_words(rw, last_carreira, page_index)
                if r is not None and pending_course_name:
                    # If the extracted course name seems truncated (e.g., empty because the
                    # name lives on the previous line), prepend the pending line.
                    if len(r.curso_nome) < 5 or r.curso_nome in {"de", "da", "do", "e"}:
                        r = Row(
                            carreira=r.carreira,
                            curso=r.curso,
                            curso_nome=_cleanup_spaces(f"{pending_course_name} {r.curso_nome}"),
                            tipo=r.tipo,
                            periodo=r.periodo,
                            campus=r.campus,
                            page=r.page,
                        )
                    pending_course_name = None
                if r is not None:
                    rows.append(r)

    # De-dup (some pages can repeat career headers / multi-line artifacts)
    uniq = {(r.carreira, r.curso, r.campus): r for r in rows}
    return list(uniq.values())


def write_csv(rows: Iterable[Row], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["CODIGO", "CURSO", "TIPO", "PERIODO", "CAMPUS"])

        # Pivot/merge by CODIGO: if the same CADIGO appears with both B and L,
        # emit it once with TIPO = Bacharelado/Licenciatura.
        by_codigo: dict[str, dict] = {}
        for r in rows:
            key = r.codigo
            entry = by_codigo.get(key)
            if entry is None:
                by_codigo[key] = {
                    "CURSO": r.curso_nome,
                    "TIPOS": {r.tipo},
                    "PERIODO": r.periodo,
                    "CAMPUS": r.campus,
                }
            else:
                entry["TIPOS"].add(r.tipo)
                # Keep first-seen for the other fields (they should match).

        def tipo_from_set(s: set[str]) -> str:
            ss = {x.upper() for x in s}
            if ss == {"B", "L"}:
                return "B/L"
            if "B" in ss:
                return "B"
            if "L" in ss:
                return "L"
            return "/".join(sorted(ss))

        def sort_key(codigo: str) -> Tuple[int, int]:
            a, b = codigo.split("-", 1)
            return (int(a), int(b))

        for codigo in sorted(by_codigo.keys(), key=sort_key):
            e = by_codigo[codigo]
            tipo_raw = tipo_from_set(e["TIPOS"])
            w.writerow([
                codigo,
                e["CURSO"],
                _tipo_label(tipo_raw),
                e["PERIODO"],
                e["CAMPUS"],
            ])


def validate_sequence(rows: Iterable[Row]) -> List[str]:
    """Validate that within each carreira, curso numbers are sequential.

        Interpretation aligned to the PDF layout: courses are sequential *within the same carreira*;
        carreira codes themselves are not guaranteed to be consecutive (+/- 1), so boundary checks
        are based on "carreira changed" rather than numeric adjacency.

        For current line with curso=n:
            Previous line must be:
                - (same carreira AND curso == n-1) OR
                - (previous carreira != current carreira) OR
                - BOF

            Next line must be:
                - (same carreira AND curso == n+1) OR
                - (next carreira != current carreira) OR
                - EOF

    IMPORTANT: since we're extracting only a page range (8..14), a carreira's courses may be
    partially present (e.g., a page might show courses 20..42 only). So we enforce sequential
    constraints only within the same carreira *and same PDF page*.
    """

    ordered = sorted(rows, key=lambda r: (r.page, int(r.carreira), int(r.curso)))
    errors: List[str] = []
    if not ordered:
        return errors

    present = {(r.page, int(r.carreira), int(r.curso)) for r in ordered}

    def fmt(r: Row) -> str:
        return f"{r.codigo} | {r.curso_nome} | {r.tipo} | {r.periodo} | {r.campus}"

    for i, cur in enumerate(ordered):
        carreira_cur = int(cur.carreira)
        curso_cur = int(cur.curso)

        prev = ordered[i - 1] if i > 0 else None
        nxt = ordered[i + 1] if i + 1 < len(ordered) else None

        # Previous validation (same page + same carreira => expected curso-1)
        prev_ok = False
        if prev is None:
            prev_ok = True
        else:
            carreira_prev = int(prev.carreira)
            curso_prev = int(prev.curso)
            if prev.page != cur.page:
                prev_ok = True
            elif carreira_prev != carreira_cur:
                prev_ok = True
            elif curso_prev == curso_cur - 1:
                prev_ok = True

        if not prev_ok:
            expected_prev = (cur.page, carreira_cur, curso_cur - 1)
            errors.append(
                "PREV mismatch:"
                f"\n  cur : {fmt(cur)}"
                f"\n  prev: {fmt(prev) if prev else '<BOF>'}"
                + ("\n  expected prev curso exists in extraction: YES" if expected_prev in present else "\n  expected prev curso exists in extraction: NO")
            )

        # Next validation (same page + same carreira => expected curso+1)
        next_ok = False
        if nxt is None:
            next_ok = True
        else:
            carreira_next = int(nxt.carreira)
            curso_next = int(nxt.curso)
            if nxt.page != cur.page:
                next_ok = True
            elif carreira_next != carreira_cur:
                next_ok = True
            elif curso_next == curso_cur + 1:
                next_ok = True

        if not next_ok:
            expected_next = (cur.page, carreira_cur, curso_cur + 1)
            errors.append(
                "NEXT mismatch:"
                f"\n  cur : {fmt(cur)}"
                f"\n  next: {fmt(nxt) if nxt else '<EOF>'}"
                + ("\n  expected next curso exists in extraction: YES" if expected_next in present else "\n  expected next curso exists in extraction: NO")
            )

    return errors


def main() -> None:
    if not PDF_PATH.exists():
        raise SystemExit(f"PDF not found: {PDF_PATH}")

    rows = extract_rows(PDF_PATH)

    seq_errors = validate_sequence(rows)
    if seq_errors:
        print(f"[VALIDATION] Sequence errors: {len(seq_errors)}")
        # Print up to 50 to avoid flooding.
        for e in seq_errors[:50]:
            print(e)
            print("-")
        raise SystemExit(2)
    else:
        print("[VALIDATION] Sequence OK")

    write_csv(rows, OUT_CSV_PATH)

    # Small sanity output
    print(f"Extracted {len(rows)} rows")
    print(f"Wrote: {OUT_CSV_PATH}")
    for r in sorted(rows, key=lambda x: (int(x.carreira), int(x.curso)))[:10]:
        print(r.codigo, r.curso_nome, _tipo_label(r.tipo), r.periodo, r.campus)


if __name__ == "__main__":
    main()
