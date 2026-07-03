"""Reference loader — usaspending_search_schema_dictionary: the per-COLUMN schema dictionary for the
two USAspending *_search rpt tables (award_search, subaward_search) whose denormalized matview
columns the USAspending DATA Element Crosswalk (DEC) does NOT document (the 118 award_search + 129
subaward_search columns uncovered by usaspending_data_dictionary).

AUTHORITATIVE SOURCE — the usaspending-api repo (the contracts the tables are built from), cloned
LIVE (same source usaspending_api_catalog uses):
  usaspending_api/search/delta_models/{award_search,subaward_search}.py
     → AWARD_SEARCH_COLUMNS / SUBAWARD_SEARCH_COLUMNS = {col: {postgres, delta, gold}} (the column
       list + authoritative postgres/delta types).
  usaspending_api/search/models/{award_search,subaward_search}.py
     → Django model help_text (sparse per-field definitions).
Definitions are then filled by joining each column to the DEC (active/usaspending_data_dictionary)
on the download/db element name — so a covered column inherits the authoritative DEC definition, and
the residual definitional gap (rpt-derived denormalized columns) is explicitly flagged.

GRAIN  1 row per (dataset, column). row_ord synthetic key. ~354 rows (151 award + 203 subaward).
SoR    s3://data-sink/active/usaspending_search_schema_dictionary/  (Lance v2.1, mode=overwrite)
COLS   row_ord, dataset, column_name, postgres_type, delta_type, gold, help_text,
       dec_element, dec_grouping, definition, definition_source, source, source_vintage, ingested_at

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'requests>=2.32' \
      python3 pipelines/reference/materialize_usaspending_search_schema.py <build|verify>
"""
from __future__ import annotations

import ast
import datetime as dt
import io
import json
import os
import re
import sys
import tarfile

ACTIVE = "s3://data-sink/active"
DATA_STORAGE_VERSION = "2.1"
REPO_TARBALL = "https://github.com/fedspendingtransparency/usaspending-api/archive/refs/heads/master.tar.gz"
BASE = "usaspending-api-master/usaspending_api/search/"
URI = os.environ.get("USA_SEARCH_SCHEMA_URI", f"{ACTIVE}/usaspending_search_schema_dictionary/")
DEC_URI = os.environ.get("USA_DATA_DICTIONARY_URI", f"{ACTIVE}/usaspending_data_dictionary/")
SPEC = [("award_search", "AWARD_SEARCH_COLUMNS", "delta_models/award_search.py", "models/award_search.py"),
        ("subaward_search", "SUBAWARD_SEARCH_COLUMNS", "delta_models/subaward_search.py", "models/subaward_search.py")]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _norm(x):
    return (x or "").strip().lower()


def _extract_columns(src, varname):
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == varname for t in node.targets):
            return ast.literal_eval(node.value)
    raise RuntimeError(f"{varname} not found")


def _help_texts(src):
    tree = ast.parse(src)
    out = {}
    for cls in [n for n in tree.body if isinstance(n, ast.ClassDef)]:
        for stmt in cls.body:
            if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call):
                name = getattr(stmt.targets[0], "id", None)
                for kw in stmt.value.keywords:
                    if kw.arg == "help_text" and isinstance(kw.value, ast.Constant):
                        out[name] = kw.value.value
    return out


def _dec_maps(so):
    """DEC lookups: download/db element name -> (element, definition, grouping), per award & subaward."""
    import lance
    dd = lance.dataset(DEC_URI, storage_options=so).scanner(
        columns=["element", "definition", "grouping", "dl_award_element", "dl_subaward_element", "db_element"]
    ).to_table().to_pylist()
    award, sub = {}, {}
    for r in dd:
        val = (r["element"], r["definition"], r["grouping"])
        for k in (r["db_element"],):            # db element applies to both, lowest precedence
            if k:
                award.setdefault(_norm(k), val); sub.setdefault(_norm(k), val)
        if r["dl_award_element"]:
            award[_norm(r["dl_award_element"])] = val      # download element wins
        if r["dl_subaward_element"]:
            sub[_norm(r["dl_subaward_element"])] = val
    return award, sub


def _award_derivations(src):
    """award_search columns are built in the PySpark AwardSearch DataFrame — each output column is an
    expression `.alias("col")`. Extract col -> the source of the aliased expression (real provenance)."""
    tree = ast.parse(src); out = {}
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "alias"
                and node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
            col = node.args[0].value
            expr = ast.get_source_segment(src, node.func.value)
            if expr:
                out.setdefault(col, " ".join(expr.split())[:400])
    return out


def _const_str(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):  # f-string: keep literal parts, mark interpolations
        return "".join(v.value if isinstance(v, ast.Constant) else " {…} " for v in node.values)
    return None


def _split_top_commas(s):
    items, depth, buf, q = [], 0, [], None
    for ch in s:
        if q:
            buf.append(ch); q = None if ch == q else q; continue
        if ch in "'\"":
            q = ch; buf.append(ch); continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            items.append("".join(buf)); buf = []
        else:
            buf.append(ch)
    if buf:
        items.append("".join(buf))
    return items


def _subaward_derivations(src):
    """subaward_search is loaded by `subaward_search_load_sql_string` — a SQL SELECT of `<expr> AS
    <col>`. Split top-level commas and keep every item that ends in `AS <col>`."""
    tree = ast.parse(src); sql = None
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(getattr(t, "id", None) == "subaward_search_load_sql_string" for t in n.targets):
            sql = _const_str(n.value)
    if not sql:
        return {}
    out = {}
    for item in _split_top_commas(sql):
        m = re.search(r'\bAS\s+"?([a-z_][a-z0-9_]*)"?\s*$', item.strip(), re.I)
        if m:
            expr = " ".join(item.strip()[:m.start()].strip().strip(",").split())
            if expr:
                out.setdefault(m.group(1), expr[:400])
    return out


def _derivations(read, dataset):
    try:
        if dataset == "award_search":
            return _award_derivations(read("delta_models/dataframes/award_search.py"))
        return _subaward_derivations(read("delta_models/subaward_search.py"))
    except Exception as e:  # noqa: BLE001
        log(f"  derivation extract failed for {dataset}: {e}")
        return {}


def build():
    import requests, pyarrow as pa, lance
    so = _r2_so()
    log(f"clone usaspending-api repo (LIVE): {REPO_TARBALL}")
    resp = requests.get(REPO_TARBALL, timeout=300); resp.raise_for_status()
    tf = tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz")
    read = lambda p: tf.extractfile(tf.getmember(BASE + p)).read().decode("utf-8", "replace")
    award_dec, sub_dec = _dec_maps(so)
    log(f"DEC join maps: award={len(award_dec)} sub={len(sub_dec)}")

    ingested = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = []
    for dataset, var, dm, mm in SPEC:
        cols = _extract_columns(read(dm), var)
        ht = _help_texts(read(mm))
        decmap = award_dec if dataset == "award_search" else sub_dec
        derivmap = _derivations(read, dataset)
        filled = deriv_n = 0
        for col, meta in cols.items():
            dec = decmap.get(_norm(col))
            help_text = ht.get(col)
            if help_text:
                definition, dsrc = help_text, "help_text"
            elif dec:
                definition, dsrc = dec[1], "dec"
            else:
                definition, dsrc = None, "none"
            if dsrc != "none":
                filled += 1
            deriv = derivmap.get(col)
            if deriv:
                deriv_n += 1
            rows.append({
                "dataset": dataset, "column_name": col,
                "postgres_type": meta.get("postgres"), "delta_type": meta.get("delta"),
                "gold": str(meta.get("gold")) if "gold" in meta else None,
                "help_text": help_text,
                "dec_element": dec[0] if dec else None, "dec_grouping": dec[2] if dec else None,
                "definition": definition, "definition_source": dsrc,
                "derivation_expr": deriv,
                "derivation_source": ("dataframes_alias" if dataset == "award_search" else "load_sql_as") if deriv else None,
                "source": REPO_TARBALL, "source_vintage": "usaspending_api_master",
                "ingested_at": ingested,
            })
        undoc = sum(1 for c in cols if not decmap.get(_norm(c)) and not ht.get(c) and not derivmap.get(c))
        log(f"  {dataset}: {len(cols)} cols · {filled} defined · {deriv_n} with derivation_expr · {undoc} fully undocumented")

    fields = ["dataset", "column_name", "postgres_type", "delta_type", "gold", "help_text",
              "dec_element", "dec_grouping", "definition", "definition_source",
              "derivation_expr", "derivation_source",
              "source", "source_vintage", "ingested_at"]
    schema = pa.schema([("row_ord", pa.int32())] + [(c, pa.string()) for c in fields])
    data = {"row_ord": list(range(len(rows)))}
    data.update({c: [r.get(c) for r in rows] for c in fields})
    tbl = pa.table(data, schema=schema)
    lance.write_dataset(tbl, URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(URI, storage_options=so)
    for c in ["row_ord", "column_name", "dec_element"]:
        ds.create_scalar_index(c, index_type="BTREE", replace=True); log(f"  BTREE ✓ {c}")
    for c in ["dataset", "gold", "definition_source", "derivation_source"]:
        ds.create_scalar_index(c, index_type="BITMAP", replace=True); log(f"  BITMAP ✓ {c}")
    log(f"DONE → {URI} rows={tbl.num_rows}")
    return {"uri": URI, "rows": tbl.num_rows}


def verify():
    import lance, collections
    so = _r2_so()
    ds = lance.dataset(URI, storage_options=so)
    t = ds.scanner(columns=["dataset", "definition_source", "column_name"]).to_table()
    ds_c = collections.Counter(t.column("dataset").to_pylist())
    src_c = collections.Counter(zip(t.column("dataset").to_pylist(), t.column("definition_source").to_pylist()))
    out = {"uri": URI, "rows": ds.count_rows(), "cols": len([f.name for f in ds.schema]),
           "by_dataset": dict(ds_c),
           "definition_fill": {f"{d}/{s}": n for (d, s), n in sorted(src_c.items())},
           "indices": [getattr(i, "name", str(i)) for i in ds.list_indices()],
           "spot_check": ds.scanner(
               columns=["dataset", "column_name", "postgres_type", "definition_source", "definition"],
               filter="column_name IN ('federal_action_obligation','business_categories','award_ts_vector','recipient_name')").to_table().to_pylist()}
    print(json.dumps(out, indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)"); sys.exit(2)


if __name__ == "__main__":
    main()
