"""LLM pass over the staffing-research role-token residuals → grounded token map.

Input: the ``roles_unresolved`` arrays in ``active/staffing_market_inputs``
(the tokens the deterministic pass could not place — vague qualifiers,
multi-word practice areas, niche titles). One LLM call per chunk classifies
each token:

    {token: {"occupational": bool, "canonical_titles": [...], "soc_major": "XX"|null}}

GROUNDING RULE — the model never mints SOC codes. Its ``canonical_titles``
(plain common job titles) are re-probed against ``occupation_alias_lookup``;
only alias-table hits become ``soc_codes``. The model's only free assignment is
the 2-digit SOC major group, drawn from a closed list in the prompt.

Output: ``s3://data-sink/active/staffing_role_token_map/`` — one row per
distinct normalized token (BTREE token_norm), provenance ``llm_map_v1`` +
model id. The normalizer consumes this map on its next run; re-running THIS
script after more research lands only classifies NEW residual tokens (the map
is read back and extended — landed classifications are never re-asked).

Run:
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/llm_map_staffing_role_tokens.py
"""
from __future__ import annotations

import json
import os
import re
import sys

import lance
import pyarrow as pa

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

INPUTS_URI = "s3://data-sink/active/staffing_market_inputs/"
ALIAS_URI = "s3://data-sink/active/occupation_alias_lookup/"
MAP_URI = "s3://data-sink/active/staffing_role_token_map/"
MODEL = "claude-sonnet-5"
CHUNK = 40

SOC_MAJORS = {
    "11": "Management", "13": "Business & Financial Operations", "15": "Computer & Mathematical",
    "17": "Architecture & Engineering", "19": "Life/Physical/Social Science",
    "21": "Community & Social Service", "23": "Legal", "25": "Education",
    "27": "Arts/Design/Media", "29": "Healthcare Practitioners & Technical",
    "31": "Healthcare Support", "33": "Protective Service", "35": "Food Prep & Serving",
    "37": "Building & Grounds Cleaning/Maintenance", "39": "Personal Care & Service",
    "41": "Sales", "43": "Office & Administrative Support", "45": "Farming/Fishing/Forestry",
    "47": "Construction & Extraction", "49": "Installation/Maintenance/Repair",
    "51": "Production", "53": "Transportation & Material Moving",
}

PROMPT = """You classify phrases from staffing-agency websites describing what they place.
For EACH input phrase return an object:
  "occupational": true if the phrase denotes people/roles/occupations being placed (even vaguely), false if it is not about roles at all (e.g. an industry, a service line, marketing fluff).
  "canonical_titles": up to 2 COMMON U.S. job titles that best represent the phrase (plain titles like "supply chain analyst", "registered nurse"). Empty list if none apply.
  "soc_major": the best-fit 2-digit SOC major group code from this closed list, or null if not occupational:
{majors}
Return STRICT JSON: an object keyed by the exact input phrase. No prose.
Input phrases:
{phrases}"""


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s.lower())).strip()


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def main() -> None:
    import anthropic

    so = _r2_storage_options()
    inputs = lance.dataset(INPUTS_URI, storage_options=so)
    residual = sorted({
        _norm(t)
        for r in inputs.to_table(columns=["roles_unresolved"]).to_pylist()
        for t in r["roles_unresolved"] if _norm(t)
    })

    # read back existing map — never re-ask a landed classification
    existing: dict[str, dict] = {}
    try:
        prior = lance.dataset(MAP_URI, storage_options=so)
        existing = {r["token_norm"]: r for r in prior.to_table().to_pylist()}
    except Exception:
        pass
    todo = [t for t in residual if t not in existing]
    print(f"residual tokens {len(residual):,} | already mapped {len(existing):,} | to classify {len(todo):,}")

    client = anthropic.Anthropic()
    majors_txt = "\n".join(f"  {k}: {v}" for k, v in SOC_MAJORS.items())
    classified: dict[str, dict] = {}
    in_toks = out_toks = 0
    for i in range(0, len(todo), CHUNK):
        chunk = todo[i:i + CHUNK]
        msg = client.messages.create(
            model=MODEL, max_tokens=4000,
            messages=[{"role": "user", "content": PROMPT.format(
                majors=majors_txt, phrases=json.dumps(chunk, ensure_ascii=False))}],
        )
        in_toks += msg.usage.input_tokens
        out_toks += msg.usage.output_tokens
        text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text").strip()
        text = re.sub(r"^```(json)?|```$", "", text, flags=re.M).strip()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            print(f"  chunk {i//CHUNK}: JSON parse failure, skipped ({len(chunk)} tokens)")
            continue
        for tok in chunk:
            v = parsed.get(tok)
            if isinstance(v, dict):
                classified[tok] = v
    print(f"classified {len(classified):,} | tokens in/out {in_toks:,}/{out_toks:,}")

    # grounding: canonical titles → alias table → SOC codes
    alias_ds = lance.dataset(ALIAS_URI, storage_options=so)
    alias_to_soc: dict[str, set[str]] = {}
    for r in alias_ds.to_table(columns=["alias_norm", "code_type", "code"]).to_pylist():
        if r["code_type"] == "soc" and r["alias_norm"] and r["code"]:
            alias_to_soc.setdefault(r["alias_norm"], set()).add(r["code"])

    rows = list(existing.values())
    grounded = 0
    for tok, v in classified.items():
        titles = [t for t in (v.get("canonical_titles") or []) if isinstance(t, str)]
        socs: set[str] = set()
        for t in titles:
            socs.update(alias_to_soc.get(_norm(t), set()))
        if socs:
            grounded += 1
        major = v.get("soc_major")
        rows.append({
            "token_norm": tok,
            "occupational": bool(v.get("occupational")),
            "canonical_titles": titles,
            "soc_codes": sorted(socs),
            "soc_major": major if major in SOC_MAJORS else None,
            "model": MODEL,
            "provenance": "llm_map_v1",
        })
    print(f"grounded to exact SOC via alias table: {grounded:,} of {len(classified):,} new")

    schema = pa.schema([
        ("token_norm", pa.string()), ("occupational", pa.bool_()),
        ("canonical_titles", pa.list_(pa.string())), ("soc_codes", pa.list_(pa.string())),
        ("soc_major", pa.string()), ("model", pa.string()), ("provenance", pa.string()),
    ])
    tbl = pa.Table.from_pylist(rows, schema=schema)
    ds = write_indexed_dataset(tbl, MAP_URI, [("token_norm", "BTREE")], so)
    occ = sum(1 for r in rows if r["occupational"])
    print(f"published {MAP_URI} rows={ds.count_rows():,} (occupational {occ:,})")


if __name__ == "__main__":
    main()
