# The Lance Columnar File Format & On-Disk Dataset Layout

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - `github.com/lancedb/lance` → `docs/src/format/file/index.md` — the Lance file-format container spec (pages, footer, column metadata, "no row groups")
> - `github.com/lancedb/lance` → `docs/src/format/file/encoding.md` — the Lance encoding strategy (structural encodings, compression schemes, `lance-encoding:` field-metadata knobs)
> - `github.com/lancedb/lance` → `docs/src/format/file/versioning.md` — the file-format version table (0.1 / 2.0 / 2.1 / 2.2 / 2.3 / stable / next / legacy)
> - `github.com/lancedb/lance` → `docs/src/format/table/index.md` — the table format (manifest, fragments, data files, deletion files, field IDs, data evolution)
> - `github.com/lancedb/lance` → `docs/src/format/table/layout.md` — on-disk dataset directory layout, base paths, file naming, version hint
> - `github.com/lancedb/lance` → `docs/src/format/table/versioning.md` — reader/writer feature-flag bitmap
> - `github.com/lancedb/lance` → `docs/src/format/table/row_id_lineage.md` — row address vs. stable row ID semantics
> - `github.com/lancedb/lance` → `protos/table.proto` — `Manifest`, `DataFragment`, `DataFile`, `DeletionFile`, `BasePath` messages (quoted verbatim)
> - `github.com/lancedb/lance` → `protos/file.proto` — `Field` message (quoted verbatim)
> - `github.com/lancedb/lance` → `protos/file2.proto` — `ColumnMetadata` / `Page` messages (quoted verbatim)
> - `github.com/lancedb/lance` → `rust/lance-encoding/src/version.rs` — the `LanceFileVersion` enum and its `#[default]` (proves the current default is **2.1**)
> - PyPI JSON API — current released versions of `pylance`, `lancedb`, `duckdb`
>
> Note on doc-site URLs: the published spec at `https://lancedb.github.io/lance/format/` and `https://docs.lancedb.com/lance` render the same content but returned HTTP 404 to automated fetch on 2026-07-08 (site-side redirect/trailing-slash quirk). This file was therefore built from the `docs/src/format/*.md` markdown sources and `protos/*.proto` in the `lancedb/lance` repository `main` branch, which are the canonical inputs those pages are generated from.

Scope: the on-disk `.lance` columnar *file* format (page layout, footer, encodings, format versions) and the on-disk Lance *dataset* format (directory structure, fragments, data files, deletion vectors, manifests, MVCC, schema/field metadata, row IDs) — as defined upstream, independent of any specific query engine or SDK.

---

## 0. The two things called "Lance"

Lance is a **stack of interoperating specifications**, not a single format. Two of those layers matter for this file:

| Layer | What it stores | Governs |
| --- | --- | --- |
| **File format** (`.lance` files) | Column data in large pages, plus a self-describing footer. Type-agnostic container + an encoding layer on top. | Page layout, encodings, random access. Only *table readers/writers and index readers/writers* need to understand it. |
| **Table format** (a *dataset*) | A versioned collection of fragments, data files, deletion files, indices, and immutable manifests. | Fragments, MVCC, schema evolution, ACID commits, time travel. Lance calls a table a **"dataset."** |

The file format deliberately leaves table semantics (statistics, indices, deletions, versioning) *out* so those concerns can evolve independently. Above the table format sit index formats, catalog specs, and a namespace client spec — out of scope for this file (see the sibling index in `../lance/00_overview.md`).

> Relevance to core-x: the Gen-3 system of record is exactly this — Lance datasets written to object storage (`s3://data-sink/active/…` on Cloudflare R2). The file-format and table-format behaviors documented below are the ground truth for how those datasets behave under append-only writes, atomic manifest commits, and BTREE scalar indexing on resolution keys.

---

## 1. The `.lance` file format (columnar container)

### 1.1 Design goals

Three properties define the container (`docs/src/format/file/index.md`):

1. **No row groups.** Unlike Parquet, Lance has *no* row-group concept — only **pages**. Each column may have its own number of pages, and different columns may have different page counts. The spec is blunt: *"We believe the concept of row groups to be fundamentally harmful to performance."* Small row groups create "runt pages" (poor cloud-store read performance); large row groups force the writer to buffer an entire row group in RAM before writing. Lance instead relies on **partial page reads** with minimal read amplification, so a file can be split for parallel readers at **any row boundary**.
2. **Random-access-friendly encoding.** Pages are laid out so a reader can fetch a contiguous row range in a small, predictable number of I/O operations — the design point for selective filters, point lookups, vector-search follow-up reads, and non-sequential ML training sampling.
3. **Functional decomposition.** Table-level statistics and query-side indices are *not* bundled into the base file structure; they are separate index formats that evolve independently.

### 1.2 File structure

A Lance file is a container for tabular data stored in **disk pages**. Each disk page holds some rows for a *single column*. There may be one or more pages per column; different columns may have different page counts. Metadata at the end of the file describes where the pages are and how they are encoded.

- **Disk pages** — sized large enough to justify a dedicated I/O op even on cloud storage (**default recommended page size: 8 MB**). Pages are not opaque: a reader can read *part* of a page when only a subset of rows is needed; the mechanics depend on the column encoding.
- **Buffer alignment** — buffers need not be contiguous; they are referenced by **absolute offsets**. In practice Lance aligns buffers to **64-byte** boundaries (4096-byte alignment is used for direct I/O).
- **External / global buffers** — because every page is referenced by an absolute offset, non-page data may be interleaved among pages (useful for extremely large values stored out-of-line). The format also supports **global buffers** for auxiliary data — the **file schema** is typically stored in a global buffer, along with optional file indexes or column statistics. References to global buffers live in the footer.
- **Column descriptors** — at the tail of the file, one standalone protobuf message per column describes each of that column's pages and its encoding. Because each column has its own message, a reader interested in a subset of columns need not read all file metadata.
- **Offset tables + footer** — after the column descriptors come offset arrays (pointers) for the column descriptors and global buffers, then a fixed-size footer.
- **Identifiers / no types at the container level** — the container has *no concept of types*. All columns are referenced by an integer **column index**; all global buffers by an integer **global buffer index**. Types are added by the encoding layer.

### 1.3 Byte layout (verbatim from the spec)

```text
// All footer fields are unsigned integers written with little endian byte order.
//
// ├──────────────────────────────────┤
// | Data Pages                       |
// |   Data Buffer 0*                 |
// |   ...                            |
// |   Data Buffer BN*                |
// ├──────────────────────────────────┤
// | Column Metadatas                 |
// | |A| Column 0 Metadata*           |
// |     Column 1 Metadata*           |
// |     ...                          |
// |     Column CN Metadata*          |
// ├──────────────────────────────────┤
// | Column Metadata Offset Table     |
// | |B| Column 0 Metadata Position*  |
// |     Column 0 Metadata Size       |
// |     ...                          |
// |     Column CN Metadata Position  |
// |     Column CN Metadata Size      |
// ├──────────────────────────────────┤
// | Global Buffers Offset Table      |
// | |C| Global Buffer 0 Position*    |
// |     Global Buffer 0 Size         |
// |     ...                          |
// |     Global Buffer GN Position    |
// |     Global Buffer GN Size        |
// ├──────────────────────────────────┤
// | Footer                           |
// | A u64: Offset to column meta 0   |
// | B u64: Offset to CMO table       |
// | C u64: Offset to GBO table       |
// |   u32: Number of global bufs     |
// |   u32: Number of columns         |
// |   u16: Major version             |
// |   u16: Minor version             |
// |   "LANC"                         |
// ├──────────────────────────────────┤
```

The footer ends with the magic bytes **`LANC`** and carries the **u16 major / u16 minor** format version inline. `*` marks fields that must be sector-aligned when direct I/O is required.

### 1.4 Column metadata & pages (verbatim proto — `protos/file2.proto`)

```protobuf
message ColumnMetadata {
  // This describes a page of column data.
  message Page {
    // The file offsets for each of the page buffers
    repeated uint64 buffer_offsets = 1;
    // The size (in bytes) of each of the page buffers
    repeated uint64 buffer_sizes = 2;
    // Logical length (e.g. # rows) of the page
    uint64 length = 3;
    // The encoding used to encode the page
    Encoding encoding = 4;
    // The priority of the page. For tabular data this is the top-level row
    // number of the first row in the page (top-level rows do not split across pages).
    uint64 priority = 5;
  }
  // Encoding info about the column itself (how to interpret column metadata buffers).
  Encoding encoding = 1;
  // The pages in the column
  repeated Page pages = 2;
  // The file offsets of each of the column metadata buffers
  repeated uint64 buffer_offsets = 3;
  // The size (in bytes) of each of the column metadata buffers
  repeated uint64 buffer_sizes = 4;
}
```

Each `Page` carries the **first row number** (`priority`), so a reader scanning a column's pages can quickly find which pages cover a wanted row range without reading page contents.

### 1.5 Reading strategy

The reader needs the footer before reading data:

1. Read one sector from the end of the file (4 KiB for local disk, larger for cloud). Parse the footer, then read the rest of the metadata now that its size is known → **1–2 IOPS**. If the metadata size is stored elsewhere (e.g. in the table manifest — see `DataFile.file_size_bytes` below), the footer is readable in a **single IOP**.
2. Scan each column's pages to determine which are needed (each page stores its first row offset). Use the page's encoding info to compute exactly which byte ranges to fetch.

Because pages are large, there is generally no benefit to sequential reads, but a file *can* be read sequentially once its metadata is known.

### 1.6 Contrast with Parquet

| | Parquet | Lance file |
| --- | --- | --- |
| Physical partitioning unit | Row group (all columns share the same row boundaries) | **Page** per column (columns partitioned independently) |
| Splitting for parallel readers | At row-group boundaries | At **any row boundary** (partial page reads) |
| Writer memory | Must buffer a whole row group | Streams pages; no whole-row-group buffer |
| Statistics / indices | Embedded in file metadata | **Out** of the file — separate index formats |
| Random access | Coarse (row-group granularity) | Fine-grained, designed for point lookups & sampling |
| Structure encoding | Repetition/definition levels (Dremel; `0` = outermost) | Repetition/definition levels, **but `0` = inner-most item** (inverted vs. Parquet) |

---

## 2. Encoding strategy: structural encodings & compression schemes

The **encoding strategy** (`docs/src/format/file/encoding.md`) evolves faster than the container itself and is what the `data_storage_version` selects between. The 0.1 and 2.0 encoding strategies are no longer documented upstream; the content below is the **2.1+** strategy.

### 2.1 Structural encodings (page layouts)

The top-level encoding decision is the **structural encoding** (proto message `PageLayout`), which breaks data into independently-decodable units and encodes structure (struct/list validity, list offsets) using **repetition & definition levels** folded into a single buffer to minimize IOPS.

| Structural encoding | When used | Notes |
| --- | --- | --- |
| **Mini-block** | Default for "smallish" types (integers, floats, booleans, small strings) | Data split into mini-blocks; each holds a power-of-two number of values (except the last), kept `< 32 KiB` compressed. Whole mini-block must be read to get one value, so blocks are kept small. Default max **4096 values/mini-block** (`LANCE_MINIBLOCK_MAX_VALUES`, max `32768`). Rep/def levels are sliced into the mini-blocks. |
| **Full-zip** | Larger values (e.g. vector embeddings) — cutoff **256 bytes** | Values zipped together per-value; requires *transparent* compression. General compression auto-applied per value when values are `≥ 32 KiB`. |
| **Constant** | Specialized cases such as **all-null** arrays | May store a single scalar without an inline value. |

Optional transforms layered on top:
- **Dictionary encoding** — applied *before* structural encoding (so the dictionary can live in the search cache, decoded once at reader init and reused for random access without re-loading). Also called categorical encoding. Gated by `dict-divisor` / `dict-size-ratio` heuristics (§2.3).
- **Packed struct encoding** — opt-in; stores struct values row-major instead of columnar. Reduces IOPS for random access when all struct fields are read together, but prevents reading individual fields independently.

### 2.2 Compression schemes (verbatim availability table)

`✅ (x.y)` = supported since version x.y; `❓` = should be usable but not yet done; `❌` = not usable (not transparent); `☑️` = applied per-value.

| Compression | Block context | Full-zip context | Mini-block context |
| --- | --- | --- | --- |
| Flat (uncompressed fixed-width) | ✅ (2.1) | ✅ (2.1) | ✅ (2.1) |
| Variable (uncompressed var-width) | ✅ (2.1) | ✅ (2.1) | ✅ (2.1) |
| Constant | ✅ (2.1) | ❓ | ❓ |
| **Bitpacking** | ✅ (2.1) | ❓ | ✅ (2.1) |
| **FSST** | ❓ | ✅ (2.1) | ✅ (2.1) |
| **RLE** (run-length) | ✅ (2.2) | ❌ | ✅ (2.1) |
| **ByteStreamSplit** (BSS) | ❓ | ❌ | ✅ (2.1) |
| **General** (LZ4 / ZStd / Snappy…) | ✅ (2.2) | ☑️ (2.1) | ✅ (2.1) |

Technique notes (from the spec):
- **Bitpacking** — strips unused high bits (e.g. a `u32` whose max is 5000 → 13 bits/value). Mini-block uses 1024 values/block with the bit width inline.
- **FSST** — Fast Static Symbol Table; a fast, *transparent* compressor and the primary algorithm for variable-width data. One FSST symbol table per disk page, stored in the protobuf description.
- **RLE** — applied in mini-block when `run_count / num_values < threshold` (default **0.5**). Fixed-width primitives only; max 2048 values/mini-block chunk.
- **BSS** — splits multi-byte values by byte position to cluster floating-point mantissa bits; **does not shrink data by itself** and is only applied when general compression is also enabled. 32/64-bit types only (f32, f64, timestamps).
- **General** — opaque back-referencing compressors (LZ4, ZStandard, Snappy). Auto-applied only in full-zip for values `≥ 32 KiB`; otherwise opt-in via config.

### 2.3 Compression configuration (verbatim — `lance-encoding:` field metadata)

These knobs can be set programmatically through writer options **or** attached to a field via schema metadata (see §5.2).

| Key | Values | Default | Description |
| --- | --- | --- | --- |
| `lance-encoding:compression` | `lz4`, `zstd`, `none`, `fsst`, … | `none` | Opt-in to general compression; value picks the scheme. |
| `lance-encoding:compression-level` | Integers (scheme-dependent) | Varies | Higher = more compression work. `zstd` levels `0-22`; `lz4` level not exposed. |
| `lance-encoding:rle-threshold` | `0.0`–`1.0` | `0.5` | RLE applied when `runs/values < threshold`. `0.0` disables. |
| `lance-encoding:bss` | `off`, `on`, `auto` | `auto` | Byte-stream-split; only effective when general compression is also on. |
| `lance-encoding:dict-divisor` | Integers `> 1` | `2` | Unique-value budget = `num_values / divisor`. |
| `lance-encoding:dict-size-ratio` | `0.0`–`1.0` | `0.8` | Dict-encoded estimate must stay below this ratio of raw page size. |
| `lance-encoding:dict-values-compression` | `lz4`, `zstd`, `none` | `lz4` | General-compression scheme for dictionary values. |
| `lance-encoding:dict-values-compression-level` | Integers (scheme-dependent) | Varies | Level for dict-values compression. |
| `lance-encoding:general` | `off`, `on` | `off` | Whether to apply general compression. |
| `lance-encoding:packed` | Any string | Not set | Apply packed-struct encoding. |
| `lance-encoding:structural-encoding` | `miniblock`, `fullzip` | Not set | Force a structural encoding (testing only). |

Relevant environment-variable fallbacks: `LANCE_ENCODING_DICT_TOO_SMALL` (default `100`), `LANCE_ENCODING_DICT_DIVISOR` (`2`), `LANCE_ENCODING_DICT_MAX_CARDINALITY` (`100000`), `LANCE_ENCODING_DICT_SIZE_RATIO` (`0.8`), `LANCE_ENCODING_DICT_VALUES_COMPRESSION[_LEVEL]`, `LANCE_MINIBLOCK_MAX_VALUES` (`4096`, max `32768`).

> Footgun: setting `lance-encoding:bss` to `on` does nothing unless `lance-encoding:compression` is also non-`none` — BSS is a *pre-transform*, not a compressor. Same for enabling general compression per column: it is only automatic for `≥ 32 KiB` full-zip values; everything else must be opted in.

---

## 3. File-format version selector (`data_storage_version`)

Every data file inside a dataset is written at one format version. The value is chosen at write time via the **`data_storage_version`** parameter (Python) and recorded in the manifest's `DataStorageFormat { file_format, version }` (§4.1). The file's own footer stores the resolved **major.minor** numbers.

### 3.1 The real, current value set (verbatim — `docs/src/format/file/versioning.md`)

The file format uses a single version number for both the container and the encoding strategy: **major** changes when the container changes, **minor** changes when only the encoding strategy changes.

| Version | Minimal Lance version | Maximum Lance version | Description |
| --- | --- | --- | --- |
| `0.1` | Any | 0.34 (write) | Initial Lance format. **No longer writable.** |
| `2.0` | 0.16.0 | Any | Reworked file format: removed row groups; added null support for lists, fixed-size lists, and primitives. |
| `2.1` | 0.38.1 | Any | Better integer & string compression; nulls in struct fields; improved random access for nested fields. |
| `2.2` | None | Any | Newer nested-type/encoding capabilities (incl. **map** support) + 2.2-era storage features. |
| `2.3` (unstable) | None | Any | Experimental encodings for upcoming features. |
| `legacy` | N/A | N/A | Alias for `0.1`. |
| `stable` | N/A | N/A | Alias for the **default version for new datasets** in the Lance release you are running. |
| `next` | N/A | N/A | Alias for the **latest unstable** version in the release you are running. |

### 3.2 Which is the current default

**The default `data_storage_version` for new datasets is `2.1`.** Proof from `rust/lance-encoding/src/version.rs`:

```rust
pub enum LanceFileVersion {
    Legacy,
    V2_0,
    #[default]   // ← the default for new datasets
    V2_1,
    Stable,      // resolves to Self::default() == V2_1
    V2_2,
    Next,        // resolves to V2_3
    V2_3,
}
```

`stable` resolves to `V2_1`; `next` resolves to `V2_3`. In the Python SDK, `write_dataset(..., data_storage_version=None)` (the default) *"will use the latest stable version"* — i.e. `2.1` in the current release (`pylance` 8.0.0). This matches the upstream "Lance File 2.1 is Now Stable" announcement.

> Aliases are resolved by *your installed Lance release*, so `stable`/`next` are non-deterministic across environments. During a format rollout (e.g. 2.3), **pin the explicit version** (`"2.1"`, `"2.2"`) for reproducible behavior. Files written with `next` may become unreadable by newer Lance versions — never use it in production.

### 3.3 Setting it (Python)

```python
import lance
import pyarrow as pa

tbl = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

# Default: latest stable (currently 2.1)
lance.write_dataset(tbl, "s3://data-sink/active/example.lance")

# Explicit pin (recommended for reproducibility)
lance.write_dataset(tbl, "s3://data-sink/active/example.lance",
                    data_storage_version="2.1")
```

`data_storage_version` docstring (verbatim, `python/python/lance/dataset.py`):

> *"The version of the data storage format to use. Newer versions are more efficient but require newer versions of lance to read. The default (None) will use the latest stable version."*

**Deprecation:** the old boolean `use_legacy_format` is deprecated — it warns and maps `True → "legacy"`, `False → "stable"`. Use `data_storage_version` instead. Full `write_dataset` signature is documented in the sibling file `../lance/02_python_dataset_api.md`.

---

## 4. On-disk dataset layout (the table format)

A **dataset** is a versioned collection of fragments, data files, deletion files, and indices; each version is described by an **immutable manifest**. It supports ACID transactions, schema evolution, time travel, and cheap incremental updates via **MVCC**.

### 4.1 Directory structure (verbatim — `docs/src/format/table/layout.md`)

```text
{dataset_root}/
    data/
        *.lance                    -- Data files containing column data
    _versions/
        *.manifest                 -- Manifest files (one per version)
        latest_version_hint.json   -- Optional hint of the latest version
    _transactions/
        *.txn                      -- Transaction files for commit coordination
    _deletions/
        *.arrow                    -- Deletion vector files (Arrow IPC format)
        *.bin                      -- Deletion vector files (Roaring bitmap format)
    _indices/
        {UUID}/
            ...                    -- Index content (varies per index type)
    _refs/
        tags/
            *.json                 -- Tag metadata
        branches/
            *.json                 -- Branch metadata
    tree/
        {branch_name}/
            ...                    -- Branch dataset
```

Every dataset has exactly one **dataset root** — the location where it was created — containing the standard subdirectories `data/`, `_versions/`, `_deletions/`, `_indices/`, `_refs/`, `tree/`.

**File-naming conventions (verbatim):**
- **Data files:** `data/{uuid-based-filename}.lance`. The 50-char filename is derived from a 16-byte UUID (first 3 bytes → 24-char binary string, remaining 13 bytes → 26-char hex) — the high-entropy binary prefix helps S3 partition and scale throughput, minimizing throttling.
- **Deletion files:** `_deletions/{fragment_id}-{read_version}-{id}.{ext}` — `.arrow` (Arrow IPC, sparse) or `.bin` (Roaring bitmap, dense). E.g. `_deletions/42-10-a1b2c3d4.arrow`.
- **Transaction files:** `_transactions/{read_version}-{uuid}.txn`. E.g. `_transactions/5-550e8400-e29b-41d4-a716-446655440000.txn`.
- **Manifest files:** in `_versions/`, one per version. V1 and V2 manifest naming schemes exist (V2 gives more efficient opening of datasets with many versions on object stores).

**Version hint** — `_versions/latest_version_hint.json` records the latest committed version:

```json
{"version": 42}
```

It is a **pure optimization** to accelerate latest-version discovery where listing `_versions/` is expensive (a reader reads the hint, then probes higher versions with cheap HEAD requests, falling back to a full listing if the hint is missing/stale). It is *always safe to delete*, never affects correctness, and writers may choose not to write it.

### 4.2 Fragments

A **fragment** is a horizontal partition of the dataset holding a subset of rows.
- Unique `uint32` id assigned incrementally from the dataset's max fragment id.
- Consists of **one or more data files** (each providing a subset of columns) plus an optional deletion file.
- `physical_rows` tracks total rows **including tombstoned rows**; current row count = `physical_rows − deletion_file.num_deleted_rows`.
- Column subsets can be read without touching all data files; each data file is independently compressed/encoded.

**Two-dimensional storage** is the core idea: rows → fragments (one dimension), and within a fragment, columns spread across multiple data files (the other dimension). This makes **data evolution** cheap — adding a column appends a new data file to each existing fragment with values computed for existing rows, *without rewriting the table*. This is the mechanism behind cheap add-column / backfill / embedding-update workflows.

**DataFragment (verbatim — `protos/table.proto`):**

```protobuf
message DataFragment {
  // The ID of a DataFragment is unique within a dataset.
  uint64 id = 1;
  repeated DataFile files = 2;
  // File that indicates which rows, if any, should be considered deleted.
  DeletionFile deletion_file = 3;

  // A serialized RowIdSequence message (see rowids.proto), in row order.
  oneof row_id_sequence {
    bytes inline_row_ids = 5;          // if small (< 200KB), inline
    ExternalFile external_row_ids = 6; // otherwise, stored as a file
  }
  oneof last_updated_at_version_sequence {
    bytes inline_last_updated_at_versions = 7;
    ExternalFile external_last_updated_at_versions = 8;
  }
  oneof created_at_version_sequence {
    bytes inline_created_at_versions = 9;
    ExternalFile external_created_at_versions = 10;
  }
  // Number of original rows in the fragment, INCLUDING deletion tombstones.
  // Current rows = physical_rows - deletion_file.num_deleted_rows.
  uint64 physical_rows = 4;
}
```

### 4.3 Data files

Data files store column data using the Lance file format (§1). Each stores a **subset of the fragment's columns**.

**DataFile (verbatim — `protos/table.proto`):**

```protobuf
message DataFile {
  // Path relative to the dataset's URI.
  string path = 1;
  // Field/column IDs in this file. In-memory default is -1 (must not be persisted).
  // -2 = "tombstoned" (field no longer in use; id was reassigned to another data file).
  repeated int32 fields = 2;
  // Top-level column index for each field. Empty for v1 files. In v2+, one entry
  // per field in `fields`; -1 means the field has no top-level column
  // (e.g. an unpacked struct/list container whose validity is in rep/def levels).
  repeated int32 column_indices = 3;
  // The major file version used to create the file.
  uint32 file_major_version = 4;
  // The minor file version used to create the file.
  // Both 0 => a version 0.1 or 0.2 file.
  uint32 file_minor_version = 5;
  // Known size of the file on disk (bytes); used to quickly find the footer.
  // Zero => "unknown".
  uint64 file_size_bytes = 6;
  // Base-path index (for imported/cloned/multi-tier files). Key into Manifest.base_paths.
  optional uint32 base_id = 7;
}
```

**Field-ID rules (load-bearing):**
- Each data file holds a **distinct set of field IDs**. Not every schema field must appear in some data file — a field with no corresponding data file reads as entirely **`NULL`**.
- A field ID of **`-2`** is a tombstone → that column is ignored. Used when rewriting a column: the old data file's field id is set to `-2` and a new data file is appended.
- **v1** assigns field IDs by position (column index derivable from field ID). **v2+** decouples them: a field may occupy a different number of columns depending on encoding, so **`column_indices` must be used** to map field → physical column.
- **2.0 vs 2.1+ field-to-column mapping:** in **2.0**, all fields (including non-leaf struct/list containers) get sequential column indices. In **2.1+**, non-leaf fields (unpacked structs, list containers) get `-1` because their validity is folded into repetition/definition levels — only leaf fields and packed structs get real column indices.

### 4.4 Deletion files (deletion vectors)

Deletion files track deleted rows **without rewriting data files**. At most **one per fragment per version**. Readers must filter out rows whose offsets appear in the deletion file.

Two formats:
- **Arrow IPC** (`.arrow`) — a flat `Int32Array` of deleted row offsets; efficient for **sparse** deletions.
- **Roaring bitmap** (`.bin`) — compressed roaring bitmap; efficient for **dense** deletions.

Deletions can be *materialized* by rewriting data files with deleted rows removed, but that **invalidates row addresses and requires rebuilding indices** (expensive) — so soft deletes via deletion vectors are the default.

**DeletionFile (verbatim — `protos/table.proto`):**

```protobuf
message DeletionFile {
  enum DeletionFileType {
    ARROW_ARRAY = 0;  // single Int32Array of deleted offsets; .arrow extension
    BITMAP = 1;       // Roaring Bitmap of deleted offsets; .bin extension
  }
  DeletionFileType file_type = 1;
  uint64 read_version = 2;      // dataset version this file was built from
  uint64 id = 3;               // opaque id to disambiguate concurrent writers
  uint64 num_deleted_rows = 4;
  optional uint32 base_id = 7; // base-path index (imported/cloned)
}
```

### 4.5 Row IDs, row addresses & stable row IDs

Every row has **two** identifier forms (`docs/src/format/table/row_id_lineage.md`):

- **Row address** (`_rowaddr`) — the current *physical* location, a 64-bit value:
  ```text
  row_address = (fragment_id << 32) | local_row_offset
  ```
  Extract fragment + offset with bit ops. **Changes** on compaction/update. This is currently the primary identifier used by secondary indices (vector, scalar, FTS reference rows by row address).
- **Row ID** (`_rowid`) — a *logical* identifier.
  - **Stable row IDs disabled (default):** row ID **equals** the row address (not stable).
  - **Stable row IDs enabled:** each row gets a unique auto-incrementing `u64` that stays constant for the row's lifetime even as its physical address changes. On update, the logical `_rowid` is preserved and remapped to the new address; the old physical row is tombstoned via the deletion vector.

**Stable row IDs must be enabled at dataset creation** (`enable_stable_row_ids=True`); they **cannot be turned on later** for an existing dataset. Assignment uses a monotonic `next_row_id` counter in the manifest (rebased on commit conflict). Row-version tracking (`_row_created_at_version`, `_row_last_updated_at_version`) is only available when stable row IDs are on.

> Historical footgun: "row id" in older code/docs frequently meant the *physical row address* (`_rowaddr`), which is **not** stable across compaction/updates. Do not treat a non-stable `_rowid` as a persistent key.

---

## 5. Manifests, MVCC & schema

### 5.1 The manifest (one immutable file per version)

A **manifest** describes a single dataset version: the full schema (incl. nested fields), the list of fragments, a monotonic `version` number, feature flags, and an optional pointer to the index section.

**Manifest (verbatim excerpt — `protos/table.proto`):**

```protobuf
message Manifest {
  repeated lance.file.Field fields = 1;          // all fields, incl. nested
  map<string, bytes> schema_metadata = 5;
  repeated DataFragment fragments = 2;
  uint64 version = 3;                            // snapshot version number
  uint64 version_aux_data = 4;

  message WriterVersion { string library = 1; string version = 2;
                          optional string prerelease = 3;
                          optional string build_metadata = 4; }
  WriterVersion writer_version = 13;

  optional uint64 index_section = 6;             // file position of index metadata
  google.protobuf.Timestamp timestamp = 7;       // version creation time, UTC
  string tag = 8;

  uint64 reader_feature_flags = 9;               // see §5.3
  uint64 writer_feature_flags = 10;
  optional uint32 max_fragment_id = 11;
  string transaction_file = 12;                  // "{read_version}-{uuid}.txn"
  optional uint64 transaction_section = 21;
  uint64 next_row_id = 14;                       // only if stable_row_ids flag set

  message DataStorageFormat { string file_format = 1; string version = 2; }
  DataStorageFormat data_format = 15;            // e.g. {"lance", "2.1"}

  map<string, string> config = 16;               // "lance." keys reserved
  map<string, string> table_metadata = 19;
  reserved 17; reserved "blob_dataset_version";
  repeated BasePath base_paths = 18;             // multi-location / shallow clone
  optional string branch = 20;                   // None => main branch
}
```

Key points:
- **Every file in a given dataset version shares the same `data_format.version`** — the format version is a per-version property, recorded here.
- `transaction_file` points at the `_transactions/*.txn` that created this version.
- `writer_version` records which library/version wrote the manifest (used to detect writer-specific bugs).

### 5.2 Schema & field metadata

The schema is a list of **`Field`** messages plus a schema-metadata map. Data types correspond **1-to-1 with Apache Arrow** types. Each field (including nested) has a unique integer **id**, assigned in **depth-first order at creation**, then incrementally for newly-added fields.

**Field (verbatim — `protos/file.proto`):**

```protobuf
message Field {
  enum Type { PARENT = 0; REPEATED = 1; LEAF = 2; }
  Type type = 1;
  string name = 2;               // fully qualified name
  int32 id = 3;                  // field id (see DataFile.fields)
  int32 parent_id = 4;           // unset => top-level column

  // Parameterized Arrow logical type. PARENT => "struct". REPEATED => "list",
  // "large_list", "list.struct", "large_list.struct". LEAF => "null", "bool",
  // "int8".."uint64", "halffloat"/"float"/"double", "string"/"large_string",
  // "binary"/"large_binary", "date32:day", "date64:ms",
  // "decimal:128:{p}:{s}"/"decimal:256:{p}:{s}",
  // "time:{unit}"/"timestamp:{unit}"/"duration:{unit}" (unit s|ms|us|ns),
  // "dict:{value_type}:{index_type}:false".
  string logical_type = 5;
  bool nullable = 6;
  map<string, bytes> metadata = 10;          // extension type name/params, encoding hints

  bool unenforced_primary_key = 12;
  uint32 unenforced_primary_key_position = 13;      // 1-based; 0 => order by field id
  bool unenforced_clustering_key = 14;              // reserved; use position field
  uint32 unenforced_clustering_key_position = 15;   // 1-based; 0 => not clustering key

  // DEPRECATED (v1 only): Encoding encoding = 7; Dictionary dictionary = 8;
  //                       string extension_name = 9;  // use metadata ARROW:extension:name
  // reserved 11 ("storage_class")
}
```

- **Encoding config lives in field metadata** under the `lance-encoding:` prefix (§2.3).
- **Unenforced primary key** — set via field metadata `lance-schema:unenforced-primary-key` = `true`/`1`/`yes` (+ optional `:position`). "Unenforced" = Lance does not always validate uniqueness (use `merge_insert` to enforce). PK field must be non-nullable (with non-nullable ancestors), a leaf primitive, and not inside a list/map. Fixed once set; cannot be updated/removed.
- `Field.encoding`, `Field.dictionary`, `Field.extension_name` are **deprecated** (v1-only) — v2 chooses encodings per page and keeps dictionaries in the data files; extension types now go through `metadata` (`ARROW:extension:name`).

### 5.3 MVCC & the atomic commit model

Lance uses **copy-on-write MVCC**:
- **Fragments and data files are immutable.** A write never mutates existing files; it writes new data files and, for deletes/updates, new deletion vectors — then writes a **new manifest** referencing the new set.
- **A commit is the atomic creation of the next manifest.** Version N+1 becomes visible only when its manifest lands. Readers pinned to version N keep seeing a fully consistent snapshot (their fragments/files still exist), so concurrent readers are never disrupted mid-query — the basis for **time travel** and safe concurrent reads.
- **Feature flags** guard forward compatibility. Readers must check `reader_feature_flags`; writers must check `writer_feature_flags`. Any unrecognized set flag → return "unsupported" and refuse the operation.

**Feature-flag bitmap (verbatim — `docs/src/format/table/versioning.md`):**

| Bit | Flag | Reader req. | Writer req. | Meaning |
| --- | --- | --- | --- | --- |
| `1` | `FLAG_DELETION_FILES` | Yes | Yes | Fragments may contain deletion files (tombstones). |
| `2` | `FLAG_STABLE_ROW_IDS` | Yes | Yes | Row IDs are stable across move/update; fragments map row IDs → addresses. |
| `4` | `FLAG_USE_V2_FORMAT_DEPRECATED` | No | No | (deprecated, unused) files written with v2 format. |
| `8` | `FLAG_TABLE_CONFIG` | No | Yes | Table config present in manifest. |
| `16` | `FLAG_BASE_PATHS` | Yes | Yes | Dataset uses multiple base paths (shallow clones / multi-base). |

Flags with bit values **≥ 32 are unknown** and cause implementations to reject the dataset with "unsupported."

### 5.4 Base paths (portability / shallow clone / multi-tier)

`Manifest.base_paths` lets file references resolve to alternative storage locations. `DataFile`, `DeletionFile`, and index metadata each carry an optional `base_id` keying into this array; absent `base_id` → resolve relative to the dataset root.

```protobuf
message BasePath {
  uint32 id = 1;
  optional string name = 2;   // human-readable alias (e.g. tag ref in shallow clone)
  bool is_dataset_root = 3;   // true => append data//_deletions//_indices/ subdirs
  string path = 4;            // absolute, object-store-interpretable
}
```

This enables **hot/cold tiering**, **multi-region** data locality, and **shallow clones** (a new dataset that references a source dataset's data files without copying — only the manifest + new data files live in the clone). By default all in-root file paths are **relative**, so a dataset is ported simply by copying its root directory — no manifest edits required.

---

## 6. Relevance to core-x

> Relevance to core-x: the two behaviors this file documents that the data plane depends on are (1) **append-only immutable fragments** and (2) **atomic manifest commit**. Every ingest appends *new* `data/*.lance` files and commits a *new* immutable manifest in `_versions/`; it never mutates an existing file. That is what makes concurrent DuckDB readers on `s3://data-sink/active/…` safe against in-flight writes — a reader pinned to version N sees a consistent snapshot until the next manifest lands atomically. Two operational consequences:
>
> - **Pin `data_storage_version` explicitly** (currently `"2.1"`) on writes rather than relying on `stable`/`next`, so every R2-resident dataset is written at a deterministic, readable-by-older-clients format version regardless of which `pylance` an executor happens to have installed.
> - **Deletes/updates are soft by default** (deletion vectors in `_deletions/`), so they do not rewrite fragments and do not invalidate the BTREE scalar indices on resolution keys. Materializing deletions (rewrite) *does* invalidate row addresses and forces index rebuilds — treat compaction as an index-invalidating event (see `../lance/08_compaction_maintenance.md`).
>
> Cloudflare R2 `storage_options` and the S3-compatible endpoint config are covered in `../lance/07_storage_object_stores.md`; the DuckDB ⇆ Arrow ⇆ Lance zero-copy path is in `../lance/10_duckdb_arrow_interop.md`.

---

## 7. Current released versions (as of 2026-07-08)

| Package | Version | Released | Source |
| --- | --- | --- | --- |
| `pylance` (the Python `lance` SDK) | **8.0.0** | 2026-07-01 | PyPI JSON API |
| `lancedb` (the LanceDB database) | **0.34.0** | 2026-07-02 | PyPI JSON API |
| `duckdb` | **1.5.4** | 2026-06-17 | PyPI JSON API |

Version-gating recap: file format `2.0` needs Lance `≥ 0.16.0`; `2.1` needs `≥ 0.38.1`; RLE-in-block and General-in-block landed at encoding version `2.2`; most other 2.1-listed compressions (Flat, Variable, Bitpacking, FSST, RLE-in-mini-block, BSS) are available from `2.1`. **`2.1` is the current default** for new datasets.

---

## 8. Sibling files (this domain)

- `00_overview.md` — Lance & LanceDB — overview, ecosystem, packaging & versions
- **`01_file_format.md`** — *(this file)* the Lance columnar file format & on-disk dataset layout
- `02_python_dataset_api.md` — pylance Python SDK: `lance.dataset`, `lance.write_dataset`, `LanceDataset`
- `03_writes_appends_upserts.md` — writing data: modes, append, `merge_insert`, delete, update, add_columns, `LanceOperation` & commits
- `04_versioning_time_travel.md` — versioning, time travel, tags & `cleanup_old_versions`
- `05_scalar_indices.md` — scalar indices: BTREE, BITMAP, LABEL_LIST, INVERTED/FTS, NGRAM
- `06_vector_search.md` — vector indices & ANN search: IVF_PQ / HNSW, nprobes, refine, multivector
- `07_storage_object_stores.md` — object store config: `storage_options` for S3 / Cloudflare R2 / GCS / Azure
- `08_compaction_maintenance.md` — dataset maintenance: compaction, index optimization, fragment management
- `09_scanning_filtering.md` — scanning, filtering, projection pushdown & `take()`
- `10_duckdb_arrow_interop.md` — interop: Apache Arrow, DuckDB, Polars/pandas; reading Lance from query engines
- `11_lancedb_table_api.md` — LanceDB (the database): connect, tables, add/search, FTS, cloud/remote

---

## 9. Unverified / needs confirmation

- **Published doc-site URLs** (`https://lancedb.github.io/lance/format/`, `https://docs.lancedb.com/lance`) returned HTTP 404 to automated fetch on 2026-07-08 despite existing per web search. This file was built from the repo's `docs/src/format/*.md` and `protos/*.proto` on `main`, which are the canonical generator inputs. If you need the rendered pages, resolve the live URL in a browser (the site uses trailing-slash redirects that broke the automated fetch).
- **`file_high_level_overview.png` / `file_overview.png` / `fragment_structure.png` diagrams** referenced in the spec are images not reproduced here; the ASCII byte-layout in §1.3 is the authoritative textual equivalent.
- **`PageLayout`, `MiniBlockLayout`, `FullZipLayout`, `ConstantLayout`, `RowIdSequence`** proto message bodies are referenced by the encoding/row-lineage docs via `%%%` include directives and were not expanded field-by-field here (only their roles are documented). Expand from `protos/encodings_v2_1.proto` and `protos/rowids.proto` if byte-level page-layout detail is required.
- **`2.2` "Minimal Lance Version" is listed as `None`** upstream — meaning the version exists but the docs do not pin a first-supporting release. Treat 2.2 as available-but-not-default; confirm against your installed `pylance` before relying on map-type support.
