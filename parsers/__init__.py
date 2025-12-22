"""Parser registry/dispatcher.

We expect multiple PDF layouts over time.
Selection is based on the `imports."Faculdade"` field.

For now we only have the UFSCar parser implementation.
"""

from .dispatcher import extract_records_from_bytes_for_faculdade

__all__ = ["extract_records_from_bytes_for_faculdade"]
