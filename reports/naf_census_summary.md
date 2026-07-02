# NAF Wage-Schedule Census — Phase 0 Fetch Manifest

Source: `https://wageandsalary.dcpas.osd.mil` (DoD DCPAS Wage & Salary). Full state→area→installation→type→version cascade.

## Totals

- Census rows (finest grain): **161,070**
- States/territories walked: **53**
- Distinct NAF wage areas: **168**
- **CT (Crafts & Trades) PDFs — exact verified URL set: 5,195**
- CT worklist provenance: 5,191 cascade + 4 unioned from Latest current-snapshot (variant re-issues the cascade omitted)
- Flat-catalog cross-check: LatestSchedulesNAF=44 anchors, NAFOverseasSchedules=67 anchors
- CT synthesized-URL pattern: HEAD-verified OK for every sampled wage area ✓

## Rows by schedule type

| type | rows |
|---|---|
| PBS | 53,294 |
| PBPR | 52,836 |
| CT | 19,683 |
| Special | 14,910 |
| RSB | 11,536 |
| AS | 8,811 |

## Deepest CT histories (wage_area → version count)

| wage_area | CT versions |
|---|---|
| 002 | 49 |
| 143 | 48 |
| 010 | 47 |
| 039 | 46 |
| 043 | 46 |
| 163 | 46 |
| 011 | 45 |
| 013 | 45 |
| 055 | 45 |
| 059 | 45 |
| 101 | 45 |
| 136 | 45 |

## Per-state area coverage

| state | wage areas | census rows |
|---|---|---|
| Alabama | 7 | 3399 |
| Alaska | 1 | 789 |
| Arizona | 5 | 2965 |
| Arkansas | 1 | 615 |
| California | 21 | 12162 |
| Colorado | 2 | 1396 |
| Connecticut | 2 | 994 |
| Delaware | 3 | 3246 |
| District of Columbia | 1 | 1500 |
| Florida | 10 | 6900 |
| Georgia | 9 | 4411 |
| Guam | 1 | 1150 |
| Hawaii | 2 | 1947 |
| Idaho | 1 | 585 |
| Illinois | 6 | 3682 |
| Indiana | 9 | 6180 |
| Iowa | 7 | 3952 |
| Kansas | 3 | 1686 |
| Kentucky | 3 | 1373 |
| Louisiana | 3 | 1371 |
| Maine | 3 | 1672 |
| Maryland | 6 | 4318 |
| Massachusetts | 3 | 1732 |
| Michigan | 6 | 3671 |
| Minnesota | 5 | 2281 |
| Mississippi | 3 | 1996 |
| Missouri | 5 | 2684 |
| Montana | 2 | 926 |
| Nebraska | 3 | 1440 |
| Nevada | 3 | 1808 |
| New Hampshire | 3 | 2216 |
| New Jersey | 5 | 4500 |
| New Mexico | 3 | 1822 |
| New York | 8 | 4812 |
| North Carolina | 7 | 3877 |
| North Dakota | 4 | 1385 |
| Ohio | 11 | 6078 |
| Oklahoma | 2 | 1248 |
| Oregon | 5 | 3238 |
| Pennsylvania | 15 | 7905 |
| Puerto Rico | 1 | 410 |
| Rhode Island | 1 | 506 |
| South Carolina | 5 | 2561 |
| South Dakota | 4 | 1918 |
| Tennessee | 9 | 3583 |
| Texas | 16 | 5720 |
| Utah | 1 | 615 |
| Vermont | 5 | 3806 |
| Virginia | 8 | 6002 |
| Washington | 8 | 4298 |
| West Virginia | 12 | 7184 |
| Wisconsin | 3 | 2434 |
| Wyoming | 5 | 2121 |

## Sample verified CT PDF URLs

- `https://wageandsalary.dcpas.osd.mil/Content/NAF%20Schedules/survey-sch/001/001-011-CT.pdf`
- `https://wageandsalary.dcpas.osd.mil/Content/NAF%20Schedules/survey-sch/001/001-012-CT.pdf`
- `https://wageandsalary.dcpas.osd.mil/Content/NAF%20Schedules/survey-sch/001/001-013-CT.pdf`
- `https://wageandsalary.dcpas.osd.mil/Content/NAF%20Schedules/survey-sch/001/001-014-CT.pdf`
- `https://wageandsalary.dcpas.osd.mil/Content/NAF%20Schedules/survey-sch/001/001-015-CT.pdf`
- `https://wageandsalary.dcpas.osd.mil/Content/NAF%20Schedules/survey-sch/001/001-016-CT.pdf`
- `https://wageandsalary.dcpas.osd.mil/Content/NAF%20Schedules/survey-sch/001/001-017-CT.pdf`
- `https://wageandsalary.dcpas.osd.mil/Content/NAF%20Schedules/survey-sch/001/001-018-CT.pdf`
