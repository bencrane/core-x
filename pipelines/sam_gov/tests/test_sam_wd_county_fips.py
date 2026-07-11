"""Pinned tests for the SAM WD county → Census FIPS crosswalk resolver."""

from pipelines.sam_gov.sam_wd_county_fips import load_gazetteer, norm, resolve, strip_suffix

GAZ = load_gazetteer("\n".join([
    "STATE|STATEFP|COUNTYFP|COUNTYNS|COUNTYNAME|CLASSFP|FUNCSTAT",
    "AL|01|001|00161526|Autauga County|H1|A",
    "AL|01|049|00161555|DeKalb County|H1|A",
    "MD|24|005|01695314|Baltimore County|H1|A",
    "MD|24|510|01702381|Baltimore city|C7|F",
    "NM|35|013|00929108|Doña Ana County|H1|A",
    "MP|69|110|01805245|Saipan Municipality|H4|A",
    "VA|51|036|01480111|Charles City County|H1|A",
    "PR|72|011|01804485|Añasco Municipio|H1|A",
]))


def test_normalized_equality():
    (r,) = resolve("AL", "Autauga", GAZ)
    assert r["county_fips"] == "01001" and r["resolution_status"] == "matched"
    assert r["match_method"] == "normalized_equality"


def test_prefix_collapse_de_kalb():
    (r,) = resolve("AL", "De Kalb", GAZ)
    assert r["county_fips"] == "01049"


def test_accent_fold():
    (r,) = resolve("NM", "Dona Ana", GAZ)
    assert r["county_fips"] == "35013"
    (r,) = resolve("PR", "Anasco", GAZ)
    assert r["county_fips"] == "72011"


def test_star_picks_independent_city():
    (r,) = resolve("MD", "Baltimore*", GAZ)
    assert r["county_fips"] == "24510" and r["match_method"] == "city_county_disambiguated"
    (r,) = resolve("MD", "Baltimore", GAZ)
    assert r["county_fips"] == "24005"


def test_cm_state_alias():
    (r,) = resolve("CM", "Saipan", GAZ)
    assert r["county_fips"] == "69110"


def test_charles_city_county_suffix_collapse():
    (r,) = resolve("VA", "Charles*", GAZ)  # SAM truncation of Charles City
    assert r["county_fips"] == "51036"


def test_statewide_is_not_a_county():
    (r,) = resolve("TX", "Statewide", GAZ)
    assert r["county_fips"] is None and r["resolution_status"] == "statewide"


def test_alias_multi_successor():
    rows = resolve("AK", "Valdez-Cordova", GAZ)
    assert sorted(r["county_fips"] for r in rows) == ["02063", "02066"]
    assert all(r["match_method"] == "alias_successor" for r in rows)


def test_wrong_state_allowlisted_not_fabricated():
    (r,) = resolve("MD", "Fairfax", GAZ)
    assert r["county_fips"] is None and r["resolution_status"] == "unresolved_wrong_state"


def test_unknown_pair_fails_closed():
    (r,) = resolve("TX", "Definitely Not A County", GAZ)
    assert r["resolution_status"] == "UNRESOLVED_NEW"


def test_norm_helpers():
    assert norm("Saint Clair") == "ST CLAIR"
    assert strip_suffix(norm("Lake and Peninsula Borough")) == "LAKE AND PENINSULA"
