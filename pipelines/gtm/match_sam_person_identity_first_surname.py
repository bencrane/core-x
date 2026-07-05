"""match_sam_person_identity_first_surname — relaxed name match: first + surname × domain.

Runs residue-scoped after the exact-name_key tiers: only domain-bearing people
with no gtm_sam_person_identity row. Exact name_key×domain matching (Tier A)
requires EVERY name token to agree, so it misses two same-person-same-company
cases this tier recovers:

    middle tokens dropped   "robert a smith" vs "robert smith"   (first name equal)
    nickname first name     "bob smith"      vs "robert smith"   (curated map)

Both levers require domain exact + surname exact (last non-suffix token).
Middle names/initials are ignored on BOTH sides.

Abstention guards (all deterministic):
    same-entity collision   two residual people at one uei collide on the
                            relaxed key (john a smith / john b smith) — no way
                            to tell which is which -> both abstain
    claimed vendor row      a vendor row whose first name matches a residual
                            person EXACTLY is never given to a nickname claim
    slug uniqueness         a person whose candidates carry >1 distinct
                            LinkedIn slug abstains (covers within-vendor
                            ambiguity AND cross-vendor conflict)

match_method (self-describing) / match_score:
    {clay|blitz}_first_surname_domain_exact        0.85
    clay+blitz_first_surname_domain_exact          0.90   (both vendors, same slug)
    {clay|blitz}_nickname_surname_domain           0.85
    clay+blitz_nickname_surname_domain             0.90

Nickname map: comprehensive curated superset of the local-part map in
gtm_sam_person_firm_emails (that one stays untouched — changing it would
change email rulings). A nickname may map to several formal names
(jeff -> jeffrey|geoffrey); equivalence = any shared canonical.

Run:
    doppler run -- python3 pipelines/gtm/match_sam_person_identity_first_surname.py           # dry-run
    doppler run -- python3 pipelines/gtm/match_sam_person_identity_first_surname.py --apply   # append
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pipelines.gtm import gtm_sam_person_identity as identity
from pipelines.gtm.match_sam_person_identity import _r2_storage_options

SRC = {
    "gtm_sam_entities": "s3://data-sink/active/gtm_sam_entities/",
    "gtm_sam_people": "s3://data-sink/active/gtm_sam_people/",
    "gtm_sam_person_identity": "s3://data-sink/active/gtm_sam_person_identity/",
    "clay_find_people": "s3://data-sink/active/clay_find_people/",
    "blitz_find_people": "s3://data-sink/active/blitz_find_people/",
}

DUCKDB_MEMORY_LIMIT = os.environ.get("GTM_DUCKDB_MEM", "16GB")
SPILL_DIR = os.environ.get("GTM_DUCKDB_SPILL", "/tmp/duckdb_spill")
PERSON_SLUG_RE = r"linkedin\.com/in/([^/?#]+)"
SUFFIXES = "['jr','sr','ii','iii','iv','v','md','phd','cpa','esq','pe','mba','jd','dds']"

# formal -> nicknames (both directions expanded at load; multi-canon nicks allowed)
NICKNAMES: dict[str, list[str]] = {
    "robert": ["bob", "bobby", "rob", "robby", "bert"], "william": ["bill", "billy", "will", "willie", "liam"],
    "james": ["jim", "jimmy", "jamie"], "john": ["johnny", "jack", "jackie"], "jonathan": ["jon", "jonny"],
    "michael": ["mike", "mikey", "mick"], "richard": ["rick", "ricky", "rich", "richie", "dick"],
    "charles": ["chuck", "charlie", "chas", "chad"], "christopher": ["chris", "kit"],
    "katherine": ["kate", "katie", "kathy", "kay", "kitty", "kat"], "catherine": ["cathy", "kate", "katie", "kay"],
    "kathleen": ["kathy", "kate"], "elizabeth": ["liz", "beth", "betsy", "eliza", "lizzie", "betty", "libby"],
    "margaret": ["peggy", "meg", "maggie", "marge", "margie", "peg", "midge"],
    "jennifer": ["jen", "jenny", "jenn"], "jessica": ["jess", "jessie"], "joseph": ["joe", "joey"],
    "thomas": ["tom", "tommy"], "daniel": ["dan", "danny"], "matthew": ["matt", "matty"], "anthony": ["tony"],
    "andrew": ["andy", "drew"], "steven": ["steve"], "stephen": ["steve"], "edward": ["ed", "eddie", "ted", "ned"],
    "donald": ["don", "donny"], "kenneth": ["ken", "kenny"], "ronald": ["ron", "ronnie"],
    "timothy": ["tim", "timmy"], "gregory": ["greg"], "jeffrey": ["jeff"], "geoffrey": ["geoff", "jeff"],
    "raymond": ["ray"], "lawrence": ["larry"], "laurence": ["larry"], "gerald": ["jerry", "gerry"],
    "jerome": ["jerry"], "patricia": ["pat", "patty", "tricia", "trish"], "patrick": ["pat", "paddy"],
    "susan": ["sue", "susie", "suzy"], "suzanne": ["sue", "suzy"], "deborah": ["deb", "debbie"],
    "barbara": ["barb", "barbie"], "sandra": ["sandy"], "cynthia": ["cindy"], "pamela": ["pam"],
    "rebecca": ["becky", "becca"], "kimberly": ["kim"], "victoria": ["vicky", "vickie", "tori"],
    "nicholas": ["nick", "nicky"], "alexander": ["alex", "al"], "alexandra": ["alex", "lexi"],
    "benjamin": ["ben", "benny", "benji"], "samuel": ["sam", "sammy"], "samantha": ["sam"],
    "frederick": ["fred", "freddie"], "theodore": ["ted", "teddy", "theo"], "leonard": ["len", "lenny", "leo"],
    "albert": ["al", "bert"], "arthur": ["art", "artie"], "eugene": ["gene"], "francis": ["fran", "frank"],
    "frances": ["fran"], "harold": ["harry", "hal"], "henry": ["hank", "harry"], "herbert": ["herb"],
    "howard": ["howie"], "isaac": ["ike"], "jacob": ["jake"], "joshua": ["josh"], "judith": ["judy"],
    "louis": ["lou", "louie"], "louise": ["lou"], "martin": ["marty"], "melissa": ["mel", "missy"],
    "nathaniel": ["nate", "nat"], "nathan": ["nate"], "norman": ["norm"], "peter": ["pete"],
    "philip": ["phil"], "phillip": ["phil"], "randall": ["randy"], "randolph": ["randy"], "rodney": ["rod"],
    "russell": ["russ"], "salvatore": ["sal"], "solomon": ["sol"], "stanley": ["stan"], "stuart": ["stu"],
    "stewart": ["stu"], "terrence": ["terry"], "terence": ["terry"], "theresa": ["terry", "tess"],
    "teresa": ["terry", "tess"], "tobias": ["toby"], "valerie": ["val"], "vincent": ["vince", "vinny"],
    "walter": ["walt", "wally"], "wesley": ["wes"], "zachary": ["zach", "zack"], "abigail": ["abby"],
    "angela": ["angie"], "bradley": ["brad"], "christine": ["chris", "christy", "chrissy"],
    "christina": ["chris", "christy", "tina", "chrissy"], "constance": ["connie"], "david": ["dave", "davey"],
    "dennis": ["denny"], "dorothy": ["dot", "dottie"], "douglas": ["doug"], "emily": ["em", "emmy"],
    "ernest": ["ernie"], "florence": ["flo"], "gabriel": ["gabe"], "gilbert": ["gil"], "gwendolyn": ["gwen"],
    "jacqueline": ["jackie"], "janet": ["jan"], "janice": ["jan"], "jason": ["jay"],
    "josephine": ["jo", "josie"], "kevin": ["kev"], "kristen": ["kris"], "kristin": ["kris"],
    "lester": ["les"], "leslie": ["les"], "lucas": ["luke"], "lucille": ["lucy"], "madeline": ["maddie"],
    "marjorie": ["margie", "marge"], "maurice": ["maury"], "maxwell": ["max"], "megan": ["meg"],
    "melanie": ["mel"], "melvin": ["mel"], "mildred": ["millie"], "mitchell": ["mitch"],
    "nicole": ["nikki", "nicki"], "olivia": ["liv"], "penelope": ["penny"], "ramona": ["mona"],
    "regina": ["gina"], "reginald": ["reggie"], "roberta": ["bobbie"], "rosemary": ["rose", "rosie"],
    "rudolph": ["rudy"], "ruth": ["ruthie"], "sarah": ["sally", "sadie"], "sara": ["sally"],
    "sidney": ["sid"], "sydney": ["syd"], "sophia": ["sophie"], "stephanie": ["steph"], "sylvester": ["sly"],
    "tamara": ["tammy"], "tiffany": ["tiff"], "tyler": ["ty"], "vernon": ["vern"], "veronica": ["ronnie"],
    "virginia": ["ginny"], "vivian": ["viv"], "winifred": ["winnie"], "wilfred": ["fred"],
    "eleanor": ["ellie", "nora"], "evelyn": ["evie"], "grace": ["gracie"], "harriet": ["hattie"],
    "helen": ["nell"], "irving": ["irv"], "gordon": ["gordy"], "jeremiah": ["jerry"], "joan": ["jo"],
    "joanne": ["jo"], "leroy": ["roy"], "sebastian": ["seb"], "seymour": ["sy"], "simon": ["si"],
    "spencer": ["spence"],
}

GIVEN_SQL = "lower(strip_accents(regexp_extract({c}, '([A-Za-z]+)', 1)))"


def surname_sql(c: str) -> str:
    return (f"list_filter(string_split_regex(lower(strip_accents(coalesce({c},''))), '[^a-z]+'),"
            f" t -> len(t) >= 2 AND NOT list_contains({SUFFIXES}, t))[-1]")


def run(apply: bool) -> None:
    import duckdb
    import lance
    import pyarrow as pa

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    build_id = f"{started:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    lineage: list[dict] = []

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    os.makedirs(SPILL_DIR, exist_ok=True)
    con.execute(f"SET temp_directory='{SPILL_DIR}'")

    def opends(name: str):
        ds = lance.dataset(SRC[name], storage_options=so)
        lineage.append({"name": name, "uri": SRC[name], "version": ds.version,
                        "rows_at_read": ds.count_rows()})
        print(f"  input {name} v{ds.version} rows={ds.count_rows():,}")
        return ds

    # nickname expansion: name -> canonical(s); formals map to themselves
    pairs = set()
    for formal, nicks in NICKNAMES.items():
        pairs.add((formal, formal))
        for n in nicks:
            pairs.add((n, formal))
    con.execute("CREATE TABLE nickmap(nm VARCHAR, canon VARCHAR)")
    con.executemany("INSERT INTO nickmap VALUES (?,?)", sorted(pairs))

    print("== inputs ==")
    ent = opends("gtm_sam_entities")
    con.register("ent_scan", ent.scanner(columns=["uei", "normalized_domain"],
                                         filter="normalized_domain IS NOT NULL").to_reader())
    con.execute("CREATE TABLE ent AS SELECT uei, lower(trim(normalized_domain)) dom FROM ent_scan")

    idn = opends("gtm_sam_person_identity")
    con.register("idn_scan", idn.scanner(columns=["sam_person_id"]).to_reader())
    con.execute("CREATE TABLE ident AS SELECT DISTINCT sam_person_id FROM idn_scan "
                "WHERE sam_person_id IS NOT NULL")

    ppl = opends("gtm_sam_people")
    con.register("ppl_scan", ppl.scanner(
        columns=["sam_person_id", "uei", "name_key", "first_name", "last_name"]).to_reader())
    con.execute(f"""CREATE TABLE resid AS
        SELECT p.sam_person_id, p.uei, p.name_key, e.dom,
               {GIVEN_SQL.format(c='p.first_name')} AS gv, {surname_sql('p.last_name')} AS sn
        FROM ppl_scan p JOIN ent e USING (uei)
        WHERE p.sam_person_id NOT IN (SELECT sam_person_id FROM ident)
          AND p.first_name IS NOT NULL AND p.last_name IS NOT NULL""")
    con.execute("DELETE FROM resid WHERE gv IS NULL OR gv='' OR sn IS NULL OR sn=''")
    n_resid = con.execute("SELECT count(*) FROM resid").fetchone()[0]
    print(f"residual people at domained entities: {n_resid:,}")

    # same-entity collision guards
    con.execute("""CREATE TABLE resid_exact AS
        SELECT r.* FROM resid r
        QUALIFY count(*) OVER (PARTITION BY uei, sn, gv) = 1""")
    con.execute("""CREATE TABLE resid_canon AS
        SELECT r.sam_person_id, r.uei, r.name_key, r.dom, r.sn, r.gv, k.canon
        FROM resid r JOIN nickmap k ON k.nm = r.gv""")
    con.execute("""CREATE TABLE canon_collide AS
        SELECT uei, sn, canon FROM resid_canon
        GROUP BY 1,2,3 HAVING count(DISTINCT sam_person_id) > 1""")
    n_ex_guard = n_resid - con.execute("SELECT count(*) FROM resid_exact").fetchone()[0]
    print(f"same-entity exact-key collisions abstained: {n_ex_guard:,}")

    # vendor rows (deduped), restricted to residual domains
    clay = opends("clay_find_people")
    con.register("clay_scan", clay.scanner(
        columns=["record_id", "full_name", "domain_norm", "linkedin_url_norm"],
        filter="domain_norm IS NOT NULL AND linkedin_url_norm IS NOT NULL AND full_name IS NOT NULL").to_reader())
    con.execute(f"""CREATE TABLE vend_clay AS
        SELECT DISTINCT lower(trim(domain_norm)) dom, {GIVEN_SQL.format(c='full_name')} gv,
               {surname_sql('full_name')} sn,
               lower(regexp_extract(linkedin_url_norm, '{PERSON_SLUG_RE}', 1)) slug,
               'clay' vendor, min(record_id) OVER (PARTITION BY domain_norm, full_name, linkedin_url_norm) record_id
        FROM clay_scan WHERE lower(trim(domain_norm)) IN (SELECT DISTINCT dom FROM resid)""")
    blitz = opends("blitz_find_people")
    con.register("blitz_scan", blitz.scanner(
        columns=["record_id", "full_name", "company_domain", "person_linkedin_norm"],
        filter="company_domain IS NOT NULL AND person_linkedin_norm IS NOT NULL AND full_name IS NOT NULL").to_reader())
    con.execute(f"""CREATE TABLE vend_blitz AS
        SELECT DISTINCT lower(trim(company_domain)) dom, {GIVEN_SQL.format(c='full_name')} gv,
               {surname_sql('full_name')} sn,
               lower(regexp_extract(person_linkedin_norm, '{PERSON_SLUG_RE}', 1)) slug,
               'blitz' vendor, min(record_id) OVER (PARTITION BY company_domain, full_name, person_linkedin_norm) record_id
        FROM blitz_scan WHERE lower(trim(company_domain)) IN (SELECT DISTINCT dom FROM resid)""")
    con.execute("""CREATE TABLE vend AS
        SELECT * FROM vend_clay UNION ALL SELECT * FROM vend_blitz""")
    con.execute("DELETE FROM vend WHERE gv IS NULL OR gv='' OR sn IS NULL OR sn='' "
                "OR slug IS NULL OR slug=''")
    print("vendor rows at residual domains:",
          con.execute("SELECT vendor, count(*) FROM vend GROUP BY 1 ORDER BY 1").fetchall())

    # lever 1 — first name literally equal (middles ignored), collision-guarded
    con.execute("""CREATE TABLE cand_exact AS
        SELECT r.sam_person_id, r.uei, r.name_key, v.slug, v.vendor, v.record_id,
               'first_surname_domain_exact' AS lever
        FROM resid_exact r JOIN vend v ON v.dom = r.dom AND v.sn = r.sn AND v.gv = r.gv""")

    # vendor rows claimed by an exact residual person are off-limits to nicknames
    con.execute("""CREATE TABLE vend_claimed AS
        SELECT DISTINCT v.dom, v.sn, v.gv FROM vend v
        JOIN resid r ON r.dom = v.dom AND r.sn = v.sn AND r.gv = v.gv""")

    # lever 2 — nickname-equivalent first names, both guards applied
    con.execute("""CREATE TABLE cand_nick AS
        SELECT DISTINCT r.sam_person_id, r.uei, r.name_key, v.slug, v.vendor, v.record_id,
               'nickname_surname_domain' AS lever
        FROM resid_canon r
        JOIN nickmap vk ON vk.canon = r.canon
        JOIN vend v ON v.dom = r.dom AND v.sn = r.sn AND v.gv = vk.nm AND v.gv <> r.gv
        WHERE NOT EXISTS (SELECT 1 FROM canon_collide c
                          WHERE c.uei = r.uei AND c.sn = r.sn AND c.canon = r.canon)
          AND NOT EXISTS (SELECT 1 FROM vend_claimed w
                          WHERE w.dom = r.dom AND w.sn = r.sn AND w.gv = v.gv)
          AND r.sam_person_id NOT IN (SELECT sam_person_id FROM cand_exact)""")

    con.execute("""CREATE TABLE cand AS
        SELECT * FROM cand_exact UNION ALL SELECT * FROM cand_nick""")

    # per-person adjudication: slug must be unique across ALL candidate rows
    con.execute("""CREATE TABLE adjudged AS
        SELECT sam_person_id, min(uei) uei, min(name_key) name_key,
               count(DISTINCT slug) n_slugs, min(slug) slug,
               min(lever) lever,
               bool_or(vendor='clay')  has_clay,
               bool_or(vendor='blitz') has_blitz,
               min(record_id) record_id
        FROM cand GROUP BY 1""")
    n_ambig = con.execute("SELECT count(*) FROM adjudged WHERE n_slugs > 1").fetchone()[0]

    con.execute("""CREATE TABLE matches AS
        SELECT sam_person_id, uei, name_key, slug AS person_linkedin_url_norm,
               record_id AS match_source,
               (CASE WHEN has_clay AND has_blitz THEN 'clay+blitz'
                     WHEN has_clay THEN 'clay' ELSE 'blitz' END) || '_' || lever AS match_method,
               CASE WHEN has_clay AND has_blitz THEN 0.90 ELSE 0.85 END AS match_score
        FROM adjudged WHERE n_slugs = 1""")

    print(f"\nambiguous (>1 slug) abstained: {n_ambig:,}")
    print("matches by method:")
    for m, s, n in con.execute("""SELECT match_method, match_score, count(*)
            FROM matches GROUP BY 1,2 ORDER BY 3 DESC""").fetchall():
        print(f"  {m:44s} score={s} rows={n:,}")
    total = con.execute("SELECT count(*) FROM matches").fetchone()[0]
    print(f"TOTAL new matches: {total:,}")
    for r in con.execute("SELECT match_method, uei, person_linkedin_url_norm FROM matches LIMIT 5").fetchall():
        print("   sample:", r)

    if not apply:
        print("\nDRY-RUN — no append. Re-run with --apply.")
        return

    arrow = con.execute(f"""
        SELECT sam_person_id, uei, name_key, person_linkedin_url_norm,
               match_source, match_method, match_score,
               '{SRC["clay_find_people"]}' AS matched_against_uri,
               {[e for e in lineage if e["name"] == "clay_find_people"][0]["version"]}::BIGINT AS matched_against_version,
               TIMESTAMP '{started:%Y-%m-%d %H:%M:%S}' AS matched_at,
               '{build_id}' AS build_id
        FROM matches ORDER BY sam_person_id""").to_arrow_table()
    arrow = arrow.cast(identity.schema())
    result = identity.append_matches(arrow, match_lineage=lineage)
    print(result)


if __name__ == "__main__":
    run(apply="--apply" in sys.argv)
