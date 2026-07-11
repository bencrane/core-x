"""Pinned grammar tests for the WD rate-register parser (deterministic, no LLM)."""

from pipelines.sam_gov.sam_wd_rate_parse import parse_dba, parse_sca

SCA_DOC = """
OCCUPATION CODE - TITLE                                    FOOTNOTE    RATE

23210 - Elevator Repairer                                              60.52
14071 - Computer Programmer I                              (see 1)
16000 - Laundry, Dry-Cleaning, Pressing And Related Occupations

06500 - Retail Automotive Detailer: Imperial Beach
& San Diego (San Diego County) & El Centro
(Imperial County), CA                                                  19.24

30463 - Technician (1)                                     25.10

HEALTH & WELFARE: $16.375 per hour for all hours worked.
"""


def test_sca_single_line():
    rows, cand, issues = parse_sca(SCA_DOC)
    assert cand == 5 and len(rows) == 5 and not issues
    r = {x["occupation_code"]: x for x in rows}
    assert r["23210"]["wage_rate"] == 60.52
    assert r["23210"]["classification_title"] == "Elevator Repairer"
    assert r["23210"]["parse_note"] == "ok"


def test_sca_see_footnote():
    rows, _, _ = parse_sca(SCA_DOC)
    r = {x["occupation_code"]: x for x in rows}
    assert r["14071"]["wage_rate"] is None
    assert r["14071"]["parse_note"] == "see_footnote"
    assert r["14071"]["footnote_ref"] == "1"


def test_sca_family_header_kept_rate_null():
    rows, _, _ = parse_sca(SCA_DOC)
    r = {x["occupation_code"]: x for x in rows}
    assert r["16000"]["wage_rate"] is None
    assert r["16000"]["parse_note"] == "no_rate"


def test_sca_wrapped_multiline_entry():
    rows, _, _ = parse_sca(SCA_DOC)
    r = {x["occupation_code"]: x for x in rows}
    assert r["06500"]["wage_rate"] == 19.24
    assert r["06500"]["classification_title"].endswith("(Imperial County), CA")


def test_sca_numeric_footnote_with_rate():
    rows, _, _ = parse_sca(SCA_DOC)
    r = {x["occupation_code"]: x for x in rows}
    assert r["30463"]["wage_rate"] == 25.10
    assert r["30463"]["footnote_ref"] == "1"


DBA_DOC = """
 ELEC0026-002 06/01/2023
                    Rates          Fringes
ELECTRICIAN......................................$ 22.99                  0.00
CEMENT MASON/CONCRETE FINISHER (INCLUDING CEMENT
FINISHING)..........................................$ 21.30                  0.00
----------------------------------------------------------------
 SUFL2022-001 08/15/2022
GLAZIER.............................................$ 17.50                  0.00
VIGO COUNTIES)......................................$ 23.38                  1,315.00
Oiler...............................................$ 38.37  Employee...      9.50
"""


def test_dba_basic_and_block_attribution():
    rows, cand, issues = parse_dba(DBA_DOC)
    assert cand == 5 and len(rows) == 5 and not issues
    r = {x["classification_title"]: x for x in rows}
    e = r["ELECTRICIAN"]
    assert e["wage_rate"] == 22.99 and e["fringe"] == 0.0
    assert e["block_id"] == "ELEC0026-002" and e["rate_source"] == "dba_union"
    assert e["block_date"] == "06/01/2023"


def test_dba_wrapped_classification():
    rows, _, _ = parse_dba(DBA_DOC)
    titles = [x["classification_title"] for x in rows]
    assert "CEMENT MASON/CONCRETE FINISHER (INCLUDING CEMENT FINISHING)" in titles


def test_dba_survey_block_and_thousands_fringe():
    rows, _, _ = parse_dba(DBA_DOC)
    r = {x["classification_title"]: x for x in rows}
    assert r["GLAZIER"]["rate_source"] == "dba_survey"
    assert r["VIGO COUNTIES)"]["fringe"] == 1315.0


def test_dba_interposed_footnote_word():
    rows, _, _ = parse_dba(DBA_DOC)
    r = {x["classification_title"]: x for x in rows}
    assert r["Oiler"]["wage_rate"] == 38.37 and r["Oiler"]["fringe"] == 9.5
