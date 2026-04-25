from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import gp_workforce_chatbot_backend_agent_v8 as backend

from v8_workforce_sql_helpers import build_practice_lookup_filter, practice_hint_variants


def _extract_practice_code(_: str) -> str:
    return ""


def _sanitise(value: str, _: str) -> str:
    return value.replace("'", "''")


def test_practice_hint_variants_handle_moorcroft_centre_ctr_alias():
    variants = practice_hint_variants("Moorcroft Medical Centre practice")

    assert "Moorcroft Medical Centre" in variants
    assert "Moorcroft" in variants
    assert "Moorcroft Medical CTR" in variants


def test_practice_lookup_filter_searches_moorcroft_aliases():
    where_sql = build_practice_lookup_filter(
        "Moorcroft Medical Centre practice",
        _extract_practice_code,
        _sanitise,
    )

    assert "Moorcroft Medical Centre" in where_sql
    assert "Moorcroft Medical CTR" in where_sql
    assert "Moorcroft" in where_sql


def test_fuzzy_match_resolves_moorcroft_medical_centre_to_ctr_name():
    candidates = [
        "CENTRE PRACTICE",
        "ASHVILLE MEDICAL CENTRE PMS PRACTICE",
        "THE MOORCROFT MEDICAL CTR",
    ]

    matches = backend.fuzzy_match("Moorcroft Medical Centre", candidates, threshold=0.45, top_n=3)

    assert matches[0][0] == "THE MOORCROFT MEDICAL CTR"
    assert matches[0][1] >= 0.9


def test_specific_practice_hint_keeps_brinsley_avenue_resolvable():
    hint = backend._specific_entity_hint("Brinsley Avenue Practice", "practice")
    variants = backend._practice_hint_variants(hint)

    assert hint == "Brinsley Avenue"
    assert "Brinsley Avenue Practice" in variants
