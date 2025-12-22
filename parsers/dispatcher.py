"""PDF parser dispatcher.

Contract:
- Input: raw PDF bytes + faculdade name (from `imports."Faculdade"`).
- Output: list of dicts with keys: nome, curso, tipo, periodo.

No `campus` field: the pipeline no longer stores it.
"""

from __future__ import annotations

from typing import Callable, Iterable, Mapping, Any

from .ufscar import extract_records_from_bytes as extract_ufscar


ParserFn = Callable[[bytes], list[dict]]


def _norm_faculdade(faculdade: str | None) -> str:
    return (faculdade or "").strip().lower()


_REGISTRY: dict[str, ParserFn] = {
    # Accept a few normalizations/aliases.
    "ufscar": extract_ufscar,
    "universidade federal de são carlos": extract_ufscar,
    "universidade federal de sao carlos": extract_ufscar,
}


def extract_records_from_bytes_for_faculdade(file_bytes: bytes, faculdade: str | None) -> list[dict]:
    """Route PDF bytes to the right parser based on faculdade.

    If faculdade is unknown, we currently fall back to the UFSCar parser to keep
    the system operational (multi-parser rollout will add more explicit support).
    """

    key = _norm_faculdade(faculdade)
    fn = _REGISTRY.get(key) or extract_ufscar
    return fn(file_bytes)
