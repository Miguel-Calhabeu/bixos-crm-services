"""PDF parser dispatcher.

Contract:
- Input: raw PDF bytes + faculdade name (from `imports."Faculdade"`).
- Output: list of dicts with keys: nome, curso, tipo, periodo.

No `campus` field: the pipeline no longer stores it.
"""

from __future__ import annotations

from typing import Callable, Iterable, Mapping, Any

from .ufscar import extract_records_from_bytes as extract_ufscar
from .fuvest import extract_records_from_bytes as extract_fuvest
from .provao import extract_records_from_bytes as extract_provao
from .ifsp import extract_records_from_bytes as extract_ifsp


ParserFn = Callable[[bytes], list[dict]]


def _norm_faculdade(faculdade: str | None) -> str:
    return (faculdade or "").strip().lower()


_REGISTRY: dict[str, ParserFn] = {
    "ufscar": extract_ufscar,
    "fuvest": extract_fuvest,
    "fuvest lista de espera": extract_provao,
    "enem usp": extract_enem_usp,
    "provao paulista (fuvest)": extract_provao,
    "ifsp": extract_ifsp,
}


def extract_records_from_bytes_for_faculdade(file_bytes: bytes, faculdade: str | None) -> list[dict]:
    """Route PDF bytes to the right parser based on faculdade.

    If faculdade is unknown, we currently fall back to the UFSCar parser to keep
    the system operational (multi-parser rollout will add more explicit support).
    """

    key = _norm_faculdade(faculdade)
    fn = _REGISTRY.get(key)
    return fn(file_bytes) if fn else []
