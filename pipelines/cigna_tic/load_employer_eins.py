#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.11,<3.13"
# dependencies = [
#   "duckdb>=1.5,<2",
#   "pylance>=7",
#   "pyarrow>=17",
# ]
# ///
"""Load the top-N high-volume employer EINs from the local Form 5500 large-plan table.

The intersect key for the Cigna ToC parser is the *plan sponsor's EIN*. On Form 5500 that
is `SPONS_DFE_EIN` on the large-plan head table (`form5500_main` = F_5500, plans with
>=100 participants — the "high-volume corporate employer" universe by construction). We
rank by `TOT_ACTIVE_PARTCP_CNT` (line 6a active participants) desc and emit the top N
distinct sponsor EINs.

Normalization (load-bearing): SPONS_DFE_EIN is stored 9-wide with EFAST2 structural
leading zeros and NO hyphen (e.g. `061033195`). Cigna's ToC stamps the same EIN
*hyphenated* (`06-1033195`). Both sides are reduced to digits-only / 9-wide here and in the
parser so set-membership is apples-to-apples — without this the intersect silently returns
zero while looking complete.

Read-only against the local lake; never mutates Form 5500.

    uv run pipelines/cigna_tic/load_employer_eins.py --out /tmp/employer_eins.txt -n 50
"""
from __future__ import annotations

import argparse
import re
import sys

import duckdb
import lance

LAKE = "/Users/benjamincrane/core-x-lake/active/form5500_main.lance"
_NON_DIGIT = re.compile(r"\D+")


def norm_ein(raw: object) -> str | None:
    """Digits-only, 9-wide. Returns None for empty/degenerate values."""
    if raw is None:
        return None
    digits = _NON_DIGIT.sub("", str(raw))
    if not digits:
        return None
    # EINs are 9 digits; keep the rightmost 9 defensively, then left-pad structural zeros.
    return digits[-9:].zfill(9)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", default=LAKE, help="path to form5500_main.lance")
    ap.add_argument("-n", "--limit", type=int, default=50, help="number of EINs to emit")
    ap.add_argument("--out", required=True, help="newline-delimited EIN output file")
    ap.add_argument(
        "--manifest",
        help="optional CSV (ein,participants,sponsor) for provenance / report tables",
    )
    args = ap.parse_args()

    ds = lance.dataset(args.dataset)  # noqa: F841 — referenced by name in the SQL below

    # Projection + predicate pushdown: only the three columns leave Lance storage.
    rows = duckdb.sql(
        """
        SELECT
            SPONS_DFE_EIN                              AS ein_raw,
            any_value(SPONSOR_DFE_NAME)                AS sponsor,
            max(TRY_CAST(TOT_ACTIVE_PARTCP_CNT AS BIGINT)) AS participants
        FROM ds
        WHERE SPONS_DFE_EIN IS NOT NULL
          AND length(trim(SPONS_DFE_EIN)) > 0
          AND TRY_CAST(TOT_ACTIVE_PARTCP_CNT AS BIGINT) IS NOT NULL
        GROUP BY SPONS_DFE_EIN
        ORDER BY participants DESC NULLS LAST
        """
    ).fetchall()

    seen: set[str] = set()
    picked: list[tuple[str, int, str]] = []
    for ein_raw, sponsor, participants in rows:
        ein = norm_ein(ein_raw)
        if ein is None or ein in seen:
            continue
        seen.add(ein)
        picked.append((ein, int(participants or 0), sponsor or ""))
        if len(picked) >= args.limit:
            break

    with open(args.out, "w") as fh:
        fh.write("\n".join(ein for ein, _, _ in picked) + "\n")

    if args.manifest:
        with open(args.manifest, "w") as fh:
            fh.write("ein,participants,sponsor\n")
            for ein, participants, sponsor in picked:
                safe = sponsor.replace('"', "'")
                fh.write(f'{ein},{participants},"{safe}"\n')

    print(
        f"[load_employer_eins] {len(picked)} EINs -> {args.out} "
        f"(ranked by TOT_ACTIVE_PARTCP_CNT desc, top participant count "
        f"{picked[0][1] if picked else 0:,})",
        file=sys.stderr,
    )
    # Echo the picks so the run is self-documenting.
    for ein, participants, sponsor in picked[:10]:
        print(f"  {ein}  {participants:>9,}  {sponsor[:48]}", file=sys.stderr)
    if len(picked) > 10:
        print(f"  … +{len(picked) - 10} more", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
