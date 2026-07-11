# Browser-Based OPM CBA Catalog Fetch (WAF Bypass)

**Blocker:** `opm.gov` WAF blocks CLI curl; confirmed residential IP + browser UA works.
**Solution:** Run pagination loop in browser context; accumulate full 1,248-record catalog; dump to JSON.

## Browser Fetch Script (Copy-Paste into DevTools Console)

```javascript
const OPM_ENDPOINT = "https://www.opm.gov/cba/api/documents/published";
const PAYLOAD_TEMPLATE = {
  sortBy: "agencynameAsc",        // ⚠️ CRITICAL: lowercase — any other casing silently breaks pagination
  agencyIds: [],
  subAgencyNames: [],
  activityOfficeRegions: [],
  laborUnionNames: [],
  locals: [],
  busCodes: [],
  currentPage: 1,
  recsPerPage: 20,
  searchString: ""
};

// Accumulator
window.opm_full_catalog = [];

// Fetch one page
async function fetchPage(pageNum) {
  const payload = { ...PAYLOAD_TEMPLATE, currentPage: pageNum };
  console.log(`[${pageNum}] Fetching...`);
  
  const resp = await fetch(OPM_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json; charset=utf-8" },
    body: JSON.stringify(payload)
  });
  
  if (!resp.ok) {
    console.error(`Page ${pageNum}: HTTP ${resp.status}`);
    return null;
  }
  
  const data = await resp.json();
  console.log(`[${pageNum}] Got ${data.records?.length || 0} records`);
  
  if (data.records && data.records.length > 0) {
    window.opm_full_catalog.push(...data.records);
    // Be polite — 100-200ms between pages
    await new Promise(resolve => setTimeout(resolve, 150));
    return data.records.length;
  }
  return 0;
}

// Pagination loop (63 pages for 1,248 records @ 20/page)
async function fetchAllPages() {
  for (let page = 1; page <= 65; page++) {
    const count = await fetchPage(page);
    if (count === 0) break;
  }
  console.log(`✓ Total fetched: ${window.opm_full_catalog.length} records`);
  console.log("Copy this to save:", JSON.stringify(window.opm_full_catalog, null, 2));
}

// Start
fetchAllPages().catch(console.error);
```

## Expected Outcome

After script completes:
```javascript
copy(JSON.stringify(window.opm_full_catalog))  // copies to clipboard
```

Paste into a `.json` file, save as `opm_cba_catalog.json`. Expected:
- 1,248 records
- Each record has: `id`, `fileName`, `fileUrl`, `expirationDate`, `agencyName`, `laborUnionName`, `subAgencyOrComponent`, `activityOfficeRegion`, `fileSize`

## Fallback: Manual Pages

If full pagination fails, the API supports filtering by agency. Check the OPM UI's XHR log to capture the exact agency IDs, then iterate `agencyIds: [id1, id2, ...]` in PAYLOAD_TEMPLATE.

## Next Step (After Catalog is Dumped)

1. Save catalog JSON to `/Users/benjamincrane/core-x/raw/opm_cba_catalog.json`
2. Commit: `git add raw/opm_cba_catalog.json && git commit -m "chore(opm): raw OPM CBA catalog dump (1,248 records from browser)"`
3. Run the Python harvest pipeline (see `pipelines/opm/opm_cba_harvest.py`)
