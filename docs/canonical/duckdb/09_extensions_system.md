# Extension System — INSTALL/LOAD, autoloading, core vs community, signing

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/current/extensions/overview.html — Extension system overview: INSTALL/LOAD, repositories, autoloading, `duckdb_extensions()`, `UPDATE EXTENSIONS`.
> - https://duckdb.org/docs/current/extensions/installing_extensions.html — Repositories (core / core_nightly / community), on-disk install layout, FORCE INSTALL, `INSTALL ... FROM`, `extension_directory`.
> - https://duckdb.org/docs/current/extensions/advanced_installation_methods.html — Direct-download URL pattern, install/load from a local path, statically linked extensions.
> - https://duckdb.org/docs/current/operations_manual/securing_duckdb/securing_extensions.html — Signed vs unsigned, `allow_unsigned_extensions`, `allow_community_extensions`, autoinstall/autoload settings, lock-once semantics.
> - https://duckdb.org/community_extensions/ — Community Extensions repository: `INSTALL ... FROM community`, CI signing.
> - https://duckdb.org/docs/current/sql/meta/duckdb_table_functions.html — `duckdb_extensions()` column schema.
> - https://duckdb.org/docs/current/core_extensions/overview — Core extensions list (see also 10_core_extensions_catalog.md).

Scope: How DuckDB installs, loads, autoloads, versions, and cryptographically verifies extensions — the `core`, `core_nightly`, and `community` repositories, signed-vs-unsigned security posture, and the `duckdb_extensions()` metadata surface.

**Current released DuckDB version as of 2026-07-08: v1.5.4** (confirmed from the direct-download URL example `http://extensions.duckdb.org/v1.5.4/windows_amd64/json.duckdb_extension.gz` on the advanced-installation page).

---

## 1. Concept: installation vs loading are two distinct steps

DuckDB extensions ship as separate binaries (`*.duckdb_extension`), not compiled into the base engine. Using one is a two-phase operation:

- **INSTALL** — downloads the extension binary from a repository, verifies its signature/metadata, and stores it on disk under `~/.duckdb/extensions/` for reuse across sessions. Persistent; done once per DuckDB version + platform.
- **LOAD** — dynamically links the on-disk binary into the *current* DuckDB instance. Per-session; must be repeated every new connection/process (unless autoloaded — see §4).

```sql
INSTALL spatial;   -- download + persist to ~/.duckdb/extensions/...
LOAD spatial;      -- link into this session
```

> **Footgun — extensions cannot be unloaded or reloaded.** Once `LOAD`ed into a process, an extension stays for the life of that process. To pick up an updated binary you must restart the DuckDB process. There is no `UNLOAD`.

---

## 2. `INSTALL` / `LOAD` syntax

### 2.1 Basic form

```sql
INSTALL extension_name;
LOAD   extension_name;
```

`INSTALL httpfs;` installs from the default `core` repository at `http://extensions.duckdb.org`. Once installed, subsequent `INSTALL extension_name` calls reuse the **local** copy and do not re-download (use `FORCE INSTALL` to override — §5).

### 2.2 `INSTALL ... FROM <repository>`

Install from a named repository or an explicit URL:

```sql
INSTALL spatial FROM core_nightly;
INSTALL tarfs   FROM community;
INSTALL custom_extension FROM 'https://my-custom-extension-repository';
```

### 2.3 Set a default repository for the session

```sql
SET custom_extension_repository = 'http://nightly-extensions.duckdb.org';
```

After this, a bare `INSTALL name` resolves against the configured repository instead of `core`.

---

## 3. Extension repositories

DuckDB ships with three predefined named repositories:

| Repository name | URL | Purpose |
|---|---|---|
| `core` | `http://extensions.duckdb.org` | Official DuckDB extensions, vetted by the core team. Default source. |
| `core_nightly` | `http://nightly-extensions.duckdb.org` | Experimental / nightly builds of core extensions. |
| `community` | `http://community-extensions.duckdb.org` | Third-party extensions, built + signed + distributed by DuckDB's Community Extensions CI. |

You may also point `INSTALL ... FROM` at an arbitrary HTTPS URL that serves the extension binary layout (see §8 for the on-disk / on-wire path convention).

### 3.1 On-disk installation layout

Extensions install into a **version-and-platform-specific** directory:

```
~/.duckdb/extensions/<duckdb_version>/<platform_name>/
```

Examples:

```
~/.duckdb/extensions/v1.5.4/osx_arm64/spatial.duckdb_extension
~/.duckdb/extensions/fc2e4b26a6/linux_amd64/httpfs.duckdb_extension   # nightly builds keyed by git hash
```

Nightly builds use the DuckDB git commit hash in place of a `vX.Y.Z` version string. The consequence: **each DuckDB version keeps its own extension binaries** — upgrading DuckDB means re-installing (or re-downloading) extensions for the new version directory.

Relocate or restrict the install directory:

```sql
SET extension_directory  = '/path/to/your/extension/directory';
-- Multiple search directories (read paths):
SET extension_directories = ['/usr/lib/duckdb/extensions', '/opt/duckdb/extensions'];
```

### 3.2 Uninstalling

There is no `UNINSTALL` SQL command. Manual removal: delete the `*.duckdb_extension` binary file from the install directory directly.

---

## 4. Autoloading — core extensions that load on first use

Many **core** extensions are *autoloadable*: referencing functionality they provide triggers DuckDB to auto-install (if missing) and auto-load them transparently, with no explicit `INSTALL`/`LOAD`. Canonical example: querying an `https://` or `s3://` path auto-activates `httpfs`.

Two boolean settings govern this, both **default `true`** in the Python client and the CLI:

| Setting | Default | Effect when `true` |
|---|---|---|
| `autoinstall_known_extensions` | `true` | On first use of a known extension, DuckDB will auto-**install** (download) it if not present on disk. |
| `autoload_known_extensions` | `true` | On first use of a known extension, DuckDB will auto-**load** it into the session. |

Disable to force fully explicit, offline-safe behavior (recommended for locked-down / no-egress environments):

```sql
SET autoinstall_known_extensions = false;
SET autoload_known_extensions    = false;
```

> **Not everything autoloads.** Only extensions on DuckDB's built-in "known extensions" list are eligible. Extensions that register functions/types eagerly or that cannot be safely resolved from a query reference must be `LOAD`ed manually. Community extensions are **not** autoloaded — they always require explicit `INSTALL ... FROM community; LOAD ...`.

> **Note on client defaults.** The `autoload_known_extensions` default is documented as `true` for Python and the CLI. There is a known cross-client inconsistency tracked upstream (duckdb-r issue #582) where some client bindings historically differed. Treat "true in Python + CLI" as ground truth; verify per-client if you rely on a non-Python/non-CLI binding.

---

## 5. `FORCE INSTALL` — re-download / repository switch

`INSTALL` is idempotent against the local copy. To force a fresh download (e.g. to pull a newer build or switch which repository a name resolves from):

```sql
FORCE INSTALL spatial;                    -- re-download from core
FORCE INSTALL spatial FROM core_nightly;  -- overwrite local copy with the nightly build
```

Use this to move an extension from `core` to `core_nightly` (or back) — a plain `INSTALL` would silently keep the already-present binary.

---

## 6. Updating installed extensions

```sql
UPDATE EXTENSIONS;              -- update all installed extensions to latest
UPDATE EXTENSIONS (name1, name2);  -- update a named subset
```

`UPDATE EXTENSIONS` refreshes installed extension binaries to their latest available versions from the repository they were installed from. Because extensions cannot be reloaded in-process (§1), a running session must be restarted to actually use an updated binary.

---

## 7. Signing & security

By default DuckDB will **only load signed extensions** and verifies each binary against **built-in public keys** before loading. Two signing authorities exist:

- **Core-signed** — extensions vetted and signed by the core DuckDB team (the `core` / `core_nightly` repositories).
- **Community-signed** — open-source third-party extensions built and signed by the DuckDB Community Extensions CI, distributed via `community-extensions.duckdb.org`. The signature proves the binary was produced by that CI, not that the code is trustworthy.

### 7.1 Three security levels

| Level | Loadable extensions | How to select |
|---|---|---|
| Most restrictive | Core-signed only | `SET allow_community_extensions = false;` |
| **Default** | Core-signed + community-signed | (no action; both allowed) |
| Least restrictive | Any, including **unsigned** | `SET allow_unsigned_extensions = true;` |

```sql
-- Lock out all community extensions (only core-team-signed load):
SET allow_community_extensions = false;

-- Allow locally-built / third-party unsigned extensions (dev / self-built):
SET allow_unsigned_extensions = true;
```

### 7.2 The `-unsigned` CLI flag

`allow_unsigned_extensions` is a **start-up** setting. To enable it for the CLI client, pass the flag at launch — you cannot set it after the process has locked down:

```bash
duckdb -unsigned
```

Then within that session unsigned extensions load without signature checks. For programmatic clients, pass the config at connection time (e.g. Python: `duckdb.connect(config={'allow_unsigned_extensions': 'true'})` — see 01_python_client.md).

### 7.3 Lock-once semantics (critical)

These security settings are **one-way ratchets within a process**: once you have restricted the security posture, you cannot relax it again in the same process. Attempting to loosen a restriction (e.g. re-enable community extensions after disabling them, or turn on `allow_unsigned_extensions` after it was left off) raises an error. This prevents untrusted SQL executed later in the session from re-opening a door you closed. Set your posture at start-up.

### 7.4 Security implications (do not skip)

Installing and loading an extension **executes native code written by that extension's authors**. For community and unsigned extensions this is third-party code running with full process privileges. The upstream docs are blunt: a malicious extension could, e.g., steal credentials/crypto. Guidance:

- Only load unsigned extensions from sources you fully trust.
- **Never** load unsigned extensions fetched over plain HTTP.
- Any service that executes **untrusted SQL** should run with `SET allow_community_extensions = false;` (and not enable unsigned), locking to core-signed extensions only.

### 7.5 `allow_extensions_metadata_mismatch`

The `allow_extensions_metadata_mismatch` setting permits loading a binary whose embedded platform/version metadata does not match the running engine. Confirmed against the DuckDB configuration reference (Global Configuration Options) on 2026-07-08: description "Allow to load extensions with not compatible metadata", **default `false`**. It is not documented on the securing-extensions page itself but lives in the general configuration surface (see also 06_configuration_memory_spill.md). Loading a metadata-mismatched binary is unsafe in general — leave it `false` unless you are deliberately side-loading a known-compatible binary the engine mis-tags.

---

## 8. Community Extensions repository

The Community Extensions program (`http(s)://community-extensions.duckdb.org`) builds, signs, and hosts third-party extensions through a centralized CI, so users install them the same way as core extensions:

```sql
INSTALL waddle FROM community;
LOAD   waddle;
```

At `LOAD` time DuckDB checks that the binary was signed by the Community Extension CI key. To forbid this class entirely:

```sql
SET allow_community_extensions = false;
```

The full catalog lives at the community-extensions site and the `duckdb/community-extensions` GitHub repository.

### 8.1 Extension versioning vs DuckDB version

- **Extensions are version-pinned to a DuckDB version + platform.** The on-disk path (`~/.duckdb/extensions/<duckdb_version>/<platform>/`) and the download URL (§9) both embed the DuckDB version. An extension binary built for `v1.5.4/osx_arm64` will not be picked up by a `v1.6.x` engine — you re-install after a DuckDB upgrade.
- **Extension self-version** is independent and surfaced via `duckdb_extensions().extension_version`: `vX.Y.Z` for stable builds, a **6-character hash** for unstable/nightly builds.
- Community extensions are rebuilt against each DuckDB release; availability of a given community extension for a brand-new DuckDB version can lag the engine release.

---

## 9. Advanced / manual installation

### 9.1 Direct download URL pattern

Extension binaries are served (gzipped) at a deterministic URL. Useful for air-gapped mirroring:

```
http://extensions.duckdb.org/v<duckdb_version>/<platform_name>/<extension_name>.duckdb_extension.gz
```

Example:

```
http://extensions.duckdb.org/v1.5.4/windows_amd64/json.duckdb_extension.gz
```

### 9.2 Install from a local path

```sql
INSTALL 'path/to/httpfs.duckdb_extension';
```

> **Footgun:** the compressed `.duckdb_extension.gz` form must be **decompressed first** — `INSTALL`/`LOAD` from a path expect the uncompressed `.duckdb_extension` binary.

### 9.3 Load directly from a path (bypass the install store)

```sql
LOAD 'path/to/httpfs.duckdb_extension';
```

This skips any currently-installed copy and loads the specified binary directly. Loading unsigned local binaries requires `allow_unsigned_extensions = true` (§7). Loading a **remote compressed** file directly is not supported.

### 9.4 Statically linked extensions

Extensions can be compiled directly into a custom DuckDB build (via extension config files, per the developer docs). Such extensions report `install_mode = 'STATICALLY_LINKED'` and `install_path = '(BUILT-IN)'` in `duckdb_extensions()`, and need no `INSTALL`. See 11_quack_extension.md for how extension binaries are built.

---

## 10. Listing & inspecting extensions — `duckdb_extensions()`

`duckdb_extensions()` is a table function returning one row per known extension. Full column schema (verbatim from the metadata table-functions reference):

| Column | Type | Description |
|---|---|---|
| `extension_name` | `VARCHAR` | The name of the extension. |
| `loaded` | `BOOLEAN` | `true` if the extension is loaded, `false` if it's not loaded. |
| `installed` | `BOOLEAN` | `true` if the extension is installed, `false` if it's not installed. |
| `install_path` | `VARCHAR` | `(BUILT-IN)` if the extension is built-in, otherwise the filesystem path where the binary resides. |
| `description` | `VARCHAR` | Human-readable text describing the extension's functionality. |
| `aliases` | `VARCHAR[]` | List of alternative names for this extension. |
| `extension_version` | `VARCHAR` | The version of the extension (`vX.Y.Z` for stable versions, 6-character hash for unstable versions). |
| `install_mode` | `VARCHAR` | Installation mode: `UNKNOWN`, `REPOSITORY`, `CUSTOM_PATH`, `STATICALLY_LINKED`, `NOT_INSTALLED`, or `NULL`. |
| `installed_from` | `VARCHAR` | Name of the repository the extension was installed from, e.g. `community` or `core_nightly`. |

Common queries:

```sql
-- Everything DuckDB knows about, with state:
SELECT extension_name, loaded, installed, extension_version, installed_from, install_mode
FROM duckdb_extensions()
ORDER BY extension_name;

-- Only what is currently loaded in this session:
SELECT extension_name, extension_version
FROM duckdb_extensions()
WHERE loaded;

-- Quick human-readable catalog:
SELECT extension_name, installed, description FROM duckdb_extensions();
```

---

## 11. Autoloadable core extensions vs manual

The core extensions list (30+ extensions) is documented in full in **10_core_extensions_catalog.md**. Key ones for out-of-core → Arrow → object-storage pipelines:

- **Autoloadable / core** (load transparently on first use when `autoload_known_extensions = true`): `httpfs` (S3/R2/HTTP object storage — see 07_httpfs_s3_r2.md), `json` (05_json.md), `parquet` (built-in in most builds; 04_parquet.md), `icu`.
- **Manual-load core**: heavier extensions such as `spatial` are best installed/loaded explicitly.
- **`parquet`** is typically statically linked (`install_mode = STATICALLY_LINKED`, `install_path = (BUILT-IN)`) — no install needed.

`parquet`, `json`, `icu`, `httpfs` are the "primary" (core-supported) tier; others are best-effort "secondary".

> A `lance` extension appears on DuckDB's core extensions overview page (third-party maintained). Verify its capabilities and current availability directly before depending on it for Lance interop — see 13_lance_interop.md for the verified read/write reality. Do not assume the presence of a `lance` name on the catalog page implies full DuckDB↔Lance parity.

---

## 12. Worked example — install/load httpfs + spatial, then list

```sql
-- httpfs: object-storage reads/writes (S3 / Cloudflare R2 / HTTP).
INSTALL httpfs;
LOAD    httpfs;

-- spatial: geometry types & functions (manual-load core extension).
INSTALL spatial;
LOAD    spatial;

-- Verify what is installed and loaded:
SELECT extension_name, loaded, installed, extension_version, installed_from
FROM duckdb_extensions()
WHERE extension_name IN ('httpfs', 'spatial')
ORDER BY extension_name;
```

Expected shape (versions/paths vary by DuckDB version + platform):

| extension_name | loaded | installed | extension_version | installed_from |
|---|---|---|---|---|
| httpfs | true | true | v1.5.4 (example) | core |
| spatial | true | true | vX.Y.Z | core |

Community extension example:

```sql
INSTALL h3 FROM community;   -- Uber H3 hex-indexing, community-signed
LOAD    h3;
```

---

## 13. Deprecations, renames, and footguns

- **`working_with_extensions` page moved.** The historical `/extensions/working_with_extensions` page 404s on the current docs tree (as of 2026-07-08); its content was folded into `overview`, `installing_extensions`, and `securing_extensions`. Use those three.
- **No `UNLOAD`; no in-process reload.** Restart the process to pick up an updated binary (§1, §6).
- **Version-pinned install dirs.** Upgrading DuckDB orphans the previous version's extension binaries; re-install per new version (§3.1).
- **Compressed binaries.** `INSTALL '<path>'` needs the decompressed `.duckdb_extension`, not the `.gz` (§9.2).
- **Security ratchet is one-way.** Restrict at start-up; you cannot loosen mid-process (§7.3).
- **Autoload defaults are client-dependent.** `true` in Python + CLI; verify for other bindings (§4).
- **`-unsigned` is start-up only.** Cannot be enabled after the process has locked its security posture (§7.2).

---

## 14. Extension development

Writing your own extension (C++ template, build, signing, the `quack` reference extension) is covered in **11_quack_extension.md** — The quack Extension & the DuckDB Extension Template. Self-built extensions are unsigned and require `allow_unsigned_extensions = true` (or the `-unsigned` CLI flag) to load; distributing through the Community Extensions CI is what grants a community signature (§8).

---

> **Relevance to core-x:** The out-of-core → Arrow → Lance-on-R2 plane leans on `httpfs` for the R2 object-storage leg (07_httpfs_s3_r2.md) and on `parquet`/`json` for ephemeral transport reads. In pipeline/Modal environments, pin behavior deterministically: pre-`INSTALL` + `LOAD httpfs` explicitly rather than relying on autoload (or mirror binaries via the §9.1 direct URL for no-egress workers), and keep `autoinstall_known_extensions`/`autoload_known_extensions` at a known state so a cold worker never silently reaches the network mid-DuckDB-run. Because extension binaries are pinned to the DuckDB version, bumping DuckDB in the worker image requires re-provisioning the extension binaries for the new version directory. For any path executing untrusted SQL, lock to core-signed (`SET allow_community_extensions = false;`) at start-up — the setting cannot be relaxed later in the process.
