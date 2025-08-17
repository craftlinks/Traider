from __future__ import annotations

"""Convenience script to initialise and populate the trading_platform database.

Usage:
    python -m seed_database <path-to-json>

The JSON file must have the following structure:
    {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [1045810, "NVIDIA CORP", "NVDA", "Nasdaq"],
            ...
        ]
    }

This script is only a thin wrapper around :pymod:`traider.db.data_manager`.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Mapping, Sequence, Any

from traider.db import create_tables, add_company_and_exchange
from traider.db import add_company

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _records_from_json(raw: Any) -> Sequence[Mapping[str, Any]]:
    """Convert the *fields*/*data* JSON payload into a list of record mappings.

    The accepted structure is::

        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [1045810, "NVIDIA CORP", "NVDA", "Nasdaq"],
                ...
            ]
        }
    """
    if isinstance(raw, Mapping) and "fields" in raw and "data" in raw:
        fields: Sequence[str] = raw["fields"]  # type: ignore[index]
        rows: Sequence[Sequence[Any]] = raw["data"]  # type: ignore[index]
        return [dict(zip(fields, row)) for row in rows]

    raise ValueError("JSON must contain 'fields' and 'data' keys.")


def main(argv: list[str] | None = None) -> None:
    argv = argv or sys.argv[1:]
    if not argv:
        print("Usage: python seed_database.py <companies.json>")
        sys.exit(1)

    json_path = Path(argv[0])
    if not json_path.exists():
        logger.error("JSON file %s does not exist", json_path)
        sys.exit(1)

    create_tables()

    try:
        raw_data = json.loads(json_path.read_text())
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse JSON: %s", exc)
        sys.exit(1)

    try:
        records = _records_from_json(raw_data)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

    records_added = 0
    for record in records:
        record_lc = {str(k).lower(): v for k, v in record.items()}

        ticker = record_lc.get("ticker")
        cik = record_lc.get("cik") or record_lc.get("cik_str")
        company_name = (
            record_lc.get("name")
            or record_lc.get("title")
            or record_lc.get("company_name")
        )
        exchange = record_lc.get("exchange")

        if not all([ticker, cik, company_name]):
            logger.warning("Skipping record due to missing required fields: %s", record)
            continue

        try:
            if exchange:
                add_company_and_exchange(
                    ticker=str(ticker),
                    cik=str(cik),
                    company_name=str(company_name),
                    exchange_name=str(exchange),
                )
            else:
                add_company(
                    ticker=str(ticker),
                    cik=str(cik),
                    company_name=str(company_name),
                )
            records_added += 1
        except Exception as exc:  # pragma: no cover
            logger.error("Failed to ingest record %s: %s", record, exc)

    logger.info("Added/updated %d company records.", records_added)


if __name__ == "__main__":
    main()
