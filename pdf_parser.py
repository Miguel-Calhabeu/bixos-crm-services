"""Backwards-compatible import path for the UFSCar PDF parser.

The ingestion pipeline is being refactored to support multiple parsers.
New code should import from `api/parsers`.

This module remains so older imports don't break.
"""

try:
    from parsers.ufscar import extract_records_from_bytes
except ModuleNotFoundError:
    from api.parsers.ufscar import extract_records_from_bytes

__all__ = ["extract_records_from_bytes"]
