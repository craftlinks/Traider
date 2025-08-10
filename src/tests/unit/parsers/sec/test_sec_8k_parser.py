from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Dict

import pytest


# Ensure the project `src/` is importable when running pytest from repo root
CURRENT_FILE = Path(__file__).resolve()
SRC_DIR = CURRENT_FILE.parents[5]  # .../Trader/src
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from traider.platforms.parsers.sec.sec_8k_parser import (  # noqa: E402
    analyze_and_extract_8k,
)


FIXTURES_DIR = CURRENT_FILE.parent / "fixtures" / "8-K"

# Manually curated expected values (do not generate from the code under test)
EXPECTED: Dict[str, Dict[str, Any]] = {
    "0000815097-25-000071.txt": {
        "items": ["5.02"],
        "items_tier_map": {"5.02": 1},
        "highest_item_tier": 1,
        "primary_exhibit_type": None,
        "has_material_contract_exhibit": False,
        "fallback_used": True,
    },
    "0001628280-25-039268.txt": {
        "items": ["2.02", "9.01"],
        "items_tier_map": {"2.02": 1},
        "highest_item_tier": 1,
        "primary_exhibit_type": "EX-99.1",
        "has_material_contract_exhibit": False,
        "fallback_used": False,
    },
    "0001641172-25-022761.txt": {
        "items": ["1.01", "1.02", "9.01"],
        "items_tier_map": {"1.01": 1, "1.02": 2},
        "highest_item_tier": 1,
        "primary_exhibit_type": None,
        "has_material_contract_exhibit": True,
        "fallback_used": True,
    },
    "0001877939-25-000083.txt": {
        "items": ["2.02", "9.01"],
        "items_tier_map": {"2.02": 1},
        "highest_item_tier": 1,
        "primary_exhibit_type": "EX-99.1",
        "has_material_contract_exhibit": False,
        "fallback_used": False,
    },
    "0001213900-25-073658.txt": {
        "items": ["3.01"],
        "items_tier_map": {"3.01": 1},
        "highest_item_tier": 1,
        "primary_exhibit_type": None,
        "has_material_contract_exhibit": False,
        "fallback_used": True,
    },
    "0001193125-25-177024.txt": {
        "items": [],
        "items_tier_map": {},
        "highest_item_tier": None,
        "primary_exhibit_type": "EX-99.1",
        "has_material_contract_exhibit": False,
        "fallback_used": False,
    },
    "0000709283-25-000021.txt": {
        "items": ["4.02"],
        "items_tier_map": {"4.02": 1},
        "highest_item_tier": 1,
        "primary_exhibit_type": None,
        "has_material_contract_exhibit": False,
        "fallback_used": True,
    },
    "0001104659-25-075743.txt": {
        "items": [],
        "items_tier_map": {},
        "highest_item_tier": None,
        "primary_exhibit_type": "EX-99.1",
        "has_material_contract_exhibit": True,
        "fallback_used": False,
    },
    "0001090872-25-000028.txt": {
        "items": ["5.02", "9.01"],
        "items_tier_map": {"5.02": 1},
        "highest_item_tier": 1,
        "primary_exhibit_type": None,
        "has_material_contract_exhibit": False,
        "fallback_used": True,
    },
    "0000950170-25-105928.txt": {
        "items": ["5.02", "404", "7.01", "9.01"],
        "items_tier_map": {"5.02": 1},
        "highest_item_tier": 1,
        "primary_exhibit_type": "EX-99.1",
        "has_material_contract_exhibit": True,
        "fallback_used": False,
    },
    "0001213900-25-073717.txt": {
        "items": ["3.02", "5.03", "3.03"],
        "items_tier_map": {},
        "highest_item_tier": None,
        "primary_exhibit_type": "EX-99.1",
        "has_material_contract_exhibit": False,
        "fallback_used": False,
    },
    "0001802450-25-000008.txt": {
        "items": ["2.03", "3.02", "5.07"],
        "items_tier_map": {},
        "highest_item_tier": None,
        "primary_exhibit_type": None,
        "has_material_contract_exhibit": False,
        "fallback_used": False,
    },
}


@pytest.mark.parametrize("fixture_name", sorted(EXPECTED.keys()))
def test_analyze_and_extract_8k_from_fixture(fixture_name: str) -> None:
    fixture_path = FIXTURES_DIR / fixture_name
    assert fixture_path.exists(), f"Missing fixture: {fixture_path}"

    text = fixture_path.read_text(encoding="utf-8", errors="ignore")
    result = analyze_and_extract_8k(
        filing_text_url=f"file://{fixture_path}",  # not used when prefetched_text provided
        session=None,
        prefetched_text=text,
    )
    assert result is not None

    exp = EXPECTED[fixture_name]

    # Items and tiers
    assert result.items == exp["items"]
    assert result.items_tier_map == exp["items_tier_map"]
    assert (result.highest_item_tier or None) == exp["highest_item_tier"]

    # Exhibits and flags
    assert (result.primary_exhibit_type or None) == exp["primary_exhibit_type"]
    assert result.has_material_contract_exhibit is exp["has_material_contract_exhibit"]
    assert result.fallback_used is exp["fallback_used"]

    # Light content smoke checks
    payload = result.primary_text or result.fallback_text or ""
    if exp["primary_exhibit_type"] is not None or exp["fallback_used"]:
        # When we expect narrative content, payload should look like real text
        assert len(payload) >= 100
        assert any(ch.isalpha() for ch in payload[:400])
    else:
        # Negative path: no narrative extracted
        assert payload == ""
