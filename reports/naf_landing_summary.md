# NAF Wage-Schedule Landing — Phase 1 Fetch Report

Source: `https://wageandsalary.dcpas.osd.mil` → `s3://data-sink/landing/naf/pdfs/`. Scope: CONUS, types ['CT', 'NF', 'NONCT']; overseas excluded.

## Totals

- Worklist: **30,334**
- **Landed PDFs: 29,326** (2090.6 MiB) across 129 area dirs
- Status breakdown: {'already_landed': 5198, 'missing': 1005, 'failed': 3, 'landed': 24128}

## Landed by schedule type

| type | pdfs |
|---|---|
| PBS | 8,792 |
| PBPR | 8,172 |
| CT | 5,173 |
| Special | 2,663 |
| AS | 2,370 |
| RSB | 2,131 |
| NF | 25 |

## Sample landed R2 keys

- `s3://data-sink/landing/naf/pdfs/001/001-011-CT.pdf`
- `s3://data-sink/landing/naf/pdfs/001/001-012-CT.pdf`
- `s3://data-sink/landing/naf/pdfs/001/001-013-CT.pdf`
- `s3://data-sink/landing/naf/pdfs/001/001-014-CT.pdf`
- `s3://data-sink/landing/naf/pdfs/001/001-015-CT.pdf`
- `s3://data-sink/landing/naf/pdfs/001/001-016-CT.pdf`
