# The quack Extension & the DuckDB Extension Template (how DuckDB extensions work)

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://github.com/duckdb/extension-template — the official C/C++ DuckDB extension template repo (default branch `main`, last updated 2026-07-01); source of truth for the template layout, the example function, the build system, and loading. Files read verbatim: `docs/README.md`, `src/waddle_extension.cpp`, `src/include/waddle_extension.hpp`, `Makefile`, `CMakeLists.txt`, `extension_config.cmake`, `vcpkg.json`.
> - https://github.com/duckdb/extension-template-rs — the official (experimental) Rust extension template built on the DuckDB C Extension API. Files read: `README.md`, `src/lib.rs`.
> - https://duckdb.org/docs/current/core_extensions/quack — the current DuckDB docs page for the `quack` **core** extension (the Quack remote client/server protocol; experimental as of DuckDB v1.5.3).
> - https://duckdb.org/docs/current/quack/overview — the Quack remote protocol overview page.
> - https://github.com/duckdb/duckdb-quack — the `quack` core-extension source repo (default branch `v1.5-variegata`); `README.md` read verbatim.
> - https://duckdb.org/docs/current/extensions/overview and https://duckdb.org/community_extensions/ — extension system (INSTALL/LOAD, autoloading, unsigned extensions) and the community-extensions channel.

Scope: What "quack" actually is today (a real experimental **core** remote-protocol extension, NOT a demo), how it differs from the historical hello-world usage, and — the load-bearing part for extension development — how the official DuckDB extension template (`extension-template`, C/C++) and its Rust sibling (`extension-template-rs`) are laid out, built with vcpkg + make, loaded as unsigned `.duckdb_extension` binaries, and distributed via community extensions.

---

## 0. TL;DR / the naming correction you need first

There are **two different things** that get conflated under the word "quack." Get this straight before reading anything else:

1. **`quack` (the core extension)** — As of **DuckDB v1.5.3** (released 2026-05, current stable line is **v1.5.4**, published 2026-06-17), `quack` is a **real, shipping core extension** that adds the **Quack client/server remote protocol**: it turns a DuckDB instance into a server other DuckDB instances connect to over a network. It is **experimental** (marked to reach stable in v2.0.0). It is documented at `/docs/current/core_extensions/quack`. **It is NOT a demo and NOT the extension template's example function.** See §5.

2. **The extension-template example function** — The canonical "hello-world" scalar function historically named `quack()` and produced by `github.com/duckdb/extension-template`. **In the current template the example function is named `waddle()`, not `quack()`** (the template was renamed, presumably to free the `quack` name for the real core extension). The template's job is to teach extension development. See §1–§4.

> If a task or older doc tells you "quack is the demo scalar function from the extension template," that was true historically but is **stale**. Today the template's demo is `waddle()`, and `quack` is the remote-protocol core extension. Both facts are verified below against live source.

Current DuckDB stable release as of 2026-07-08: **v1.5.4** (`gh api repos/duckdb/duckdb/releases/latest` → `tag_name: v1.5.4`, published 2026-06-17). The v1.5 line carries the codename `variegata`.

---

## 1. What the DuckDB extension template IS

`github.com/duckdb/extension-template` is the official starter repo for building a custom DuckDB extension. Verbatim from its `docs/README.md`:

> This repository contains a template for creating a DuckDB extension. The main goal of this template is to allow users to easily develop, test and distribute their own DuckDB extension. The main branch of the template is always based on the latest stable DuckDB allowing you to try out your extension right away.

You do not clone it directly — you click **"Use this template"** on GitHub to fork it into your own repo, then clone **with submodules**:

```sh
git clone --recurse-submodules https://github.com/<you>/<your-new-extension-repo>.git
```

`--recurse-submodules` is required because DuckDB itself is pulled in as a submodule and is needed to build the extension.

### The example function today: `waddle()` (not `quack()`)

The template ships a **single example scalar function** in `src/waddle_extension.cpp`. `docs/README.md` documents it (note: the README's example output string is **stale** — see the source-of-truth callout below):

```
D select waddle('Jane') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Quack Jane 🐥 │   ← as printed in docs/README.md (STALE, see below)
└───────────────┘
```

> **Doc/source discrepancy (verified 2026-07-08):** `docs/README.md` shows `waddle('Jane')` returning `Quack Jane 🐥`, but the actual C++ source `src/waddle_extension.cpp` returns the literal `"...........🦆 " + name` (an ellipsis-dot prefix + duck emoji), and it registers **two** functions, not one: `waddle` and `waddle_openssl_version`. Trust the source. The README's "single scalar function" prose and its emoji output are behind the code.

The **verbatim registration** from `src/waddle_extension.cpp`:

```cpp
inline void WaddleScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "...........🦆 " + name.GetString());
	});
}

inline void WaddleOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Waddle " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}
```

So the two real example functions are:

| Function | Signature | Returns |
|---|---|---|
| `waddle(VARCHAR)` | `waddle(VARCHAR) → VARCHAR` | `"...........🦆 " + input` |
| `waddle_openssl_version(VARCHAR)` | `waddle_openssl_version(VARCHAR) → VARCHAR` | `"Waddle " + input + ", my linked OpenSSL version is " + OPENSSL_VERSION_TEXT` |

The second function exists purely to demonstrate **linking a vcpkg dependency** (OpenSSL) into an extension — it prints the OpenSSL version the extension was compiled against.

**The template is a teaching artifact, not a data/storage extension.** It exists to give you a compilable, testable, distributable skeleton. It does not read files, talk to object storage, or add a table format. For meaningful examples the README points to DuckDB's in-tree extensions (`github.com/duckdb/duckdb/tree/main/extension`), the test extensions (`.../test/extension`), and out-of-tree extensions under `github.com/duckdblabs`.

---

## 2. Repo layout (C/C++ template)

Top-level files/dirs in `duckdb/extension-template` (from the GitHub contents API, `main`):

```
.clang-format          # style
.clang-tidy
.editorconfig
.github/               # CI/CD workflows (MainDistributionPipeline.yml)
.gitmodules            # declares the two submodules
CMakeLists.txt         # extension build definition (see §3)
Makefile               # thin wrapper -> extension-ci-tools makefile (see §3)
LICENSE
docs/                  # README.md (the real docs), NEXT_README.md, UPDATING.md
scripts/               # bootstrap-template.py, extension-upload.sh
src/                   # waddle_extension.cpp + include/waddle_extension.hpp
test/                  # SQL tests in ./test/sql
vcpkg.json             # dependency manifest (declares openssl)
extension_config.cmake # tells DuckDB's build system which extension(s) to load
duckdb/                # SUBMODULE: core DuckDB
extension-ci-tools/    # SUBMODULE: shared build/test/deploy makefiles + vcpkg ports
```

> There is **no top-level `README.md`** rendered by the GitHub API (it 404s on the contents endpoint). The authoritative human-readable docs live at **`docs/README.md`**. Do not chase a root README.

### The two submodules (from `docs/README.md`, verbatim table)

| Name | Repository | Description |
|---|---|---|
| duckdb | https://github.com/duckdb/duckdb | Core DuckDB code required for building extensions. |
| extension-ci-tools | https://github.com/duckdb/extension-ci-tools | Reusable components for building, testing and deploying DuckDB extensions. |

Update the submodules at least once every other major LTS release to avoid CI build errors from a stale pin:

```bash
git submodule update --init --recursive
```

To pin `duckdb` to a specific commit (verbatim pattern from the docs):

```bash
cd duckdb
git fetch --all
git checkout 8e146474d7adb960c5a2941142fe4482cc7dfc08   # or any tag/branch/commit hash
cd ..
git add duckdb
git commit -m "Pin DuckDB submodule to cc7dfc08"
git push HEAD:update-submodule-branch
```

---

## 3. The C/C++ extension structure & entrypoint

### Header — `src/include/waddle_extension.hpp` (verbatim)

```cpp
#pragma once

#include "duckdb.hpp"

namespace duckdb {

class WaddleExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
```

An extension is a C++ class deriving from `duckdb::Extension`, overriding three methods:
- `void Load(ExtensionLoader &)` — register everything (scalar/table/aggregate functions, types, etc.).
- `std::string Name()` — the extension's name.
- `std::string Version() const` — the extension version string.

> **Version-gated API note:** current template uses `void Load(ExtensionLoader &loader)`. Older DuckDB extensions (pre-`ExtensionLoader`) used `void Load(DuckDB &db)` / `void Load(DatabaseInstance &instance)` and registered functions via `ExtensionUtil::RegisterFunction(instance, fn)`. If you copy an old example that references `DatabaseInstance` or `ExtensionUtil`, it will not compile against the current template — use `ExtensionLoader` and `loader.RegisterFunction(...)`.

### The registration + entrypoint — `src/waddle_extension.cpp` (verbatim, key parts)

```cpp
#define DUCKDB_EXTENSION_MAIN

#include "waddle_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

// ... WaddleScalarFun / WaddleOpenSSLVersionScalarFun (see §1) ...

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto waddle_scalar_function =
	    ScalarFunction("waddle", {LogicalType::VARCHAR}, LogicalType::VARCHAR, WaddleScalarFun);
	loader.RegisterFunction(waddle_scalar_function);

	// Register another scalar function
	auto waddle_openssl_version_scalar_function = ScalarFunction("waddle_openssl_version", {LogicalType::VARCHAR},
	                                                             LogicalType::VARCHAR, WaddleOpenSSLVersionScalarFun);
	loader.RegisterFunction(waddle_openssl_version_scalar_function);
}

void WaddleExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string WaddleExtension::Name() {
	return "waddle";
}

std::string WaddleExtension::Version() const {
#ifdef EXT_VERSION_WADDLE
	return EXT_VERSION_WADDLE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(waddle, loader) {
	duckdb::LoadInternal(loader);
}
}
```

The load-bearing pieces of the minimal skeleton:
- `#define DUCKDB_EXTENSION_MAIN` — must be defined once, in the entrypoint translation unit.
- `ScalarFunction("name", {arg LogicalTypes}, return LogicalType, cfunc_ptr)` — the constructor used to declare a scalar function.
- `loader.RegisterFunction(fn)` — registers it into the catalog.
- `DUCKDB_CPP_EXTENSION_ENTRY(<ext_name>, loader) { ... }` — the C ABI entrypoint macro DuckDB calls when the loadable extension is `LOAD`ed. This replaced the older hand-written `extern "C" void <name>_init(DatabaseInstance &db)` / `<name>_version()` pair.

### Build wiring

`Makefile` (verbatim — it is a 6-line wrapper; all real logic lives in extension-ci-tools):

```makefile
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=waddle
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
```

`extension_config.cmake` (verbatim) — tells DuckDB's build which extension to compile:

```cmake
# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(waddle
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
```

`CMakeLists.txt` (verbatim, key lines) — sets the target name, finds the vcpkg dependency, and produces **both** a static and a loadable build:

```cmake
cmake_minimum_required(VERSION 3.5)
set(TARGET_NAME waddle)
find_package(OpenSSL REQUIRED)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
project(${TARGET_NAME})
set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
include_directories(src/include)
set(EXTENSION_SOURCES src/waddle_extension.cpp)
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
```

`build_static_extension` links the extension **into** the DuckDB shell/unittest binaries; `build_loadable_extension` produces the standalone `.duckdb_extension` file. C++ standard is **C++17**.

`vcpkg.json` (verbatim) — the dependency manifest:

```json
{
	"dependencies": [ "openssl" ],
	"vcpkg-configuration": {
		"overlay-ports": [ "./extension-ci-tools/vcpkg_ports" ],
		"overlay-triplets": [ "./extension-ci-tools/toolchains" ]
	}
}
```

---

## 4. Building, running, testing, renaming (C/C++)

### 4.1 Set up vcpkg (dependency management)

> The template's example depends on OpenSSL **for instructive purposes**. If you skip vcpkg the build may fail until you remove the dependency (delete the OpenSSL usage from `vcpkg.json`, `CMakeLists.txt`, and `src/waddle_extension.cpp`). vcpkg is only required for extensions that use it.

Verbatim setup from `docs/README.md` (note the **pinned vcpkg commit**):

```shell
cd <your-working-dir-not-the-plugin-repo>
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg && git checkout ce613c41372b23b1f51333815feb3edd87ef8a8b
sh ./scripts/bootstrap.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=`pwd`/scripts/buildsystems/vcpkg.cmake
```

### 4.2 Build

```sh
make
```

Faster incremental builds (build core DuckDB once, then rapid rebuilds) — install `ccache` and `ninja` first:

```sh
GEN=ninja make
```

**Build artifacts** (verbatim paths from `docs/README.md`):

```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/<extension_name>/<extension_name>.duckdb_extension
```

- `duckdb` — the DuckDB shell with the extension **statically linked / pre-loaded**.
- `unittest` — the DuckDB test runner, extension linked in.
- `<extension_name>.duckdb_extension` — the **loadable binary** as it would be distributed.

### 4.3 Run

```sh
./build/release/duckdb
```

This shell already has the extension pre-loaded, so you can call the function directly (see §1 for the real output string).

### 4.4 Test

Primary testing is SQL tests in `./test/sql` (SQLLogicTest format):

```sh
make test
```

### 4.5 Rename the extension for your own use

After creating a repo from the template, rename the extension (rewrites files in place):

```sh
# Note: This will rewrite this file!
python3 ./scripts/bootstrap-template.py <extension_name_you_want>
```

Then rebuild; the example function is renamed to your extension name (per `docs/README.md`):

```
./build/release/duckdb
D select <extension_name_you_chose>('Jane') as result;
```

The `scripts/` dir also contains `extension-upload.sh` for deploying to a custom repository (see §6.3).

---

## 5. `quack` — the REAL core extension (remote client/server protocol)

This is what `https://duckdb.org/docs/current/core_extensions/quack` actually documents today. It is **unrelated** to the template's demo function.

**What it is** (verbatim from `duckdb/duckdb-quack` `README.md`, branch `v1.5-variegata`):

> The `quack` extension adds a client-server protocol to DuckDB. With this extension, DuckDB can act as both a server and a client to communicate over a network.

**Status:** experimental / pre-release. The core-extension docs page states (verbatim):

> As of DuckDB v1.5.3, `quack` is in an experimental state. The protocol, the function names, and implementation details are all subject to change. Quack is expected to reach stable status in DuckDB v2.0.0, scheduled for September 2026.

**It is a core extension** (built + distributed by the DuckDB team), so it **autoinstalls and autoloads on first use**. Manual install/load:

```sql
INSTALL quack;
LOAD quack;
```

**Verified usage example** (verbatim from the `duckdb-quack` README):

```sql
-- On the SERVER instance:
CALL quack_serve('quack:localhost', token = 'super_secret');
CREATE TABLE hello AS FROM VALUES ('world') v(s);
```

```sql
-- On the CLIENT instance:
CREATE SECRET (TYPE quack, TOKEN 'super_secret');
ATTACH 'quack:localhost' AS remote;
FROM remote.hello;      -- shows the remote table's contents
```

Copying data from client to server also works:

```sql
-- on client
CREATE TABLE remote.hello2 AS FROM VALUES ('world2') v(s);
-- on server
FROM hello2;
```

Confirmed API surface. The first four rows are verbatim from the authoritative `duckdb-quack` README; the remainder were confirmed 2026-07-08 against `github.com/duckdb/duckdb-quack` source (GitHub code search on branch `v1.5-variegata`) **and** the `/docs/current/quack/overview` page:

| Call / syntax | Role | Purpose |
|---|---|---|
| `CALL quack_serve('quack:<host>', token = '<secret>')` | server | Start the Quack server on this instance. |
| `CREATE SECRET (TYPE quack, TOKEN '<secret>')` | client | Store the auth token for connecting. |
| `ATTACH 'quack:<host>' AS <alias>` | client | Attach the remote server as a schema; query with `<alias>.<table>`. |
| `quack:<host>` URI scheme | both | Addresses a Quack endpoint. |
| `quack_stop(...)` | server | Stop a running Quack server. |
| `quack_query(...)` | client | Issue a query against a Quack endpoint. |
| `quack_identify(...)` | client | Identify / introspect the remote endpoint. |
| `quack_uri_parser(...)` | both | Parse a `quack:` URI. |
| `whoami()` (table macro) | server | Report the caller's identity. |
| `DETACH <alias>` | client | Detach a previously-attached Quack server. |

Additional confirmed facts from `/docs/current/quack/overview` (2026-07-08): the default port is **`9494`**; requests/responses are encoded with DuckDB's internal serialization primitives (the same code path as the WAL) rather than an interchange format (the docs reference an `application/duckdb` content type); and `SET httpfs_connection_caching = true;` can be enabled to reuse connections across requests. Function-name/port existence was cross-checked via GitHub code search against the `duckdb-quack` source: `quack_serve`, `quack_stop`, `quack_query`, `quack_identify`, and the literal `9494` all appear in the repo.

> **Still fluid:** `quack` is experimental (§0) — function names and protocol details are explicitly "subject to change" until v2.0.0. Re-fetch `/docs/current/quack/overview` before depending on the exact signatures above.

---

## 6. The extension system: loading & distributing an out-of-tree extension

This is the machinery both `waddle` (your custom extension) and any community extension ride on. Cross-reference: [09_extensions_system.md](09_extensions_system.md) (INSTALL/LOAD, autoloading, signing), [10_core_extensions_catalog.md](10_core_extensions_catalog.md) (the official core list).

### 6.1 Core vs. community vs. unsigned custom

- **Core extensions** (e.g. `httpfs`, `parquet`, `json`, and now `quack`) are built, tested, signed, and distributed by the DuckDB team; most **autoload** on first use. `INSTALL x; LOAD x;` works with no extra config.
- **Community extensions** are contributed by third parties but **built and signed in a centralized CI repo** (`github.com/duckdb/community-extensions`). They load without `allow_unsigned_extensions` because the CI signs them.
- **Your own locally-built or custom-hosted extension** is **unsigned** and therefore requires opting into unsigned extensions.

### 6.2 Loading your locally-built (unsigned) extension

DuckDB refuses to load unsigned extensions unless started with `allow_unsigned_extensions`. For the CLI, that flag is `-unsigned`:

```shell
duckdb -unsigned
```

Then load the built artifact directly by path:

```sql
LOAD '/path/to/downloaded/extension.duckdb_extension';
```

For non-CLI clients, set the config option at connect time. Python (see [01_python_client.md](01_python_client.md)):

```python
import duckdb
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD '/abs/path/to/waddle.duckdb_extension'")
con.sql("SELECT waddle('Jane')").show()
```

### 6.3 Distributing

**(a) Community extensions (recommended).** The template's default CI is designed so that an extension which builds under it can be submitted to `github.com/duckdb/community-extensions` via a PR containing a descriptor file (homebrew/vcpkg-style). After the community CI builds + signs it:

```sql
INSTALL <my_extension> FROM community;
LOAD <my_extension>;
```

`SET allow_community_extensions = false;` disables loading of anything signed with the community key, for locked-down environments.

**(b) GitHub Actions artifacts.** The template CI uploads built binaries as artifacts on every push to `main`. Download and load directly (requires `-unsigned` / `allow_unsigned_extensions`):

```sql
LOAD '/path/to/downloaded/extension.duckdb_extension';
```

**(c) Custom repository.** Host the binaries yourself and install from a URL (also requires unsigned extensions enabled):

```sql
INSTALL <my_extension> FROM 'http://my-custom-repo';
LOAD <my_extension>;
```

Deploy tooling lives in `scripts/extension-upload.sh` and `extension-ci-tools`.

### 6.4 Version pinning is strict

> Extension binaries only work for the **specific DuckDB version** they were built for.

The template targets the latest stable DuckDB on `main`. As new DuckDB versions ship you must rebuild; the workflow at `.github/workflows/MainDistributionPipeline.yml` builds for all target architectures of the pinned DuckDB version, and can be duplicated to emit binaries for multiple DuckDB versions.

---

## 7. The Rust extension option — `extension-template-rs`

`github.com/duckdb/extension-template-rs` is the official **experimental** template for **pure-Rust** extensions, built on **DuckDB's C Extension API**. Verbatim from its `README.md`:

> This is an **experimental** template for Rust based extensions based on the C Extension API of DuckDB. The goal is to turn this eventually into a stable basis for pure-Rust DuckDB extensions that can be submitted to the Community extensions repository.

Advertised features (verbatim): **No DuckDB build required · No C++ or C code required · CI/CD chain preconfigured · (Coming soon) Works with community extensions.** The "no DuckDB build required" property is the key practical difference from the C/C++ template (which compiles all of DuckDB from the submodule).

### Build flow (verbatim)

```shell
git clone --recurse-submodules <repo>
make configure     # sets up a Python venv with DuckDB + test runner; detects platform
make debug         # cargo build -> shared lib -> appends binary footer -> build/debug/...
make release       # optimized build
```

`make debug` produces a shared library in `target/debug/<shared_lib_name>`, then a script appends a binary footer to turn it into a loadable `.duckdb_extension` under `build/debug/`.

### Example functions (verbatim)

The Rust template registers a **scalar** function `rusty_echo()` and a **table** function `rusty_quack()`:

```sh
duckdb -unsigned
```

```sql
LOAD './build/debug/extension/rusty_quack/rusty_quack.duckdb_extension';
SELECT rusty_echo('Jane');
-- ┌─────────────────────┐
-- │ rusty_echo('Jane')  │
-- │       varchar       │
-- ├─────────────────────┤
-- │ 🐤 Jane 🦀 Jane     │
-- └─────────────────────┘

SELECT * FROM rusty_quack('Jane');
-- ┌─────────────────────┐
-- │       column0       │
-- │       varchar       │
-- ├─────────────────────┤
-- │ Rusty Quack Jane 🐥 │
-- └─────────────────────┘
```

### Rust structure — `src/lib.rs` (verbatim, key parts)

Scalar functions implement the `VScalar` trait; table functions implement `VTab` (with `BindData`/`InitData`):

```rust
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    ffi::duckdb_string_t,
    types::DuckString,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::{arrow::WritableVector, BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};

struct EchoScalar;
impl VScalar for EchoScalar {
    type State = ();
    fn invoke(_state: &Self::State, input: &mut DataChunkHandle, output: &mut dyn WritableVector)
        -> Result<(), Box<dyn std::error::Error>> {
        // ... reads duckdb_string_t input, writes format!("🐤 {s} 🦀 {s}")
    }
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct HelloVTab;
impl VTab for HelloVTab {
    type InitData = HelloInitData;
    type BindData = HelloBindData;
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("column0", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let name = bind.get_parameter(0).to_string();
        Ok(HelloBindData { name })
    }
    // init(...) + func(...) emit one row: format!("Rusty Quack {} 🐥", name)
}
```

The C-ABI entrypoint uses the `duckdb_entrypoint_c_api` macro (imported at top). Registration happens through a `Connection` in the entrypoint (via `con.register_scalar_function::<EchoScalar>(...)` / `con.register_table_function::<HelloVTab>(...)` — pattern per the `duckdb` Rust crate's C-API vtab support).

### Testing & version switching (verbatim)

Tests use the DuckDB Python client + SQLLogicTest format (`test/sql/<extension_name>.test`):

```shell
make test_debug     # or make test_release
```

Switch the DuckDB version under test:

```shell
make clean_all
DUCKDB_TEST_VERSION=v1.3.2 make configure
make debug
```

> `src/wasm_lib.rs` also exists — the Rust template can target DuckDB-Wasm.

---

## 8. Footguns & honest caveats

- **`quack` ≠ demo.** Do not tell an engineer "run the quack extension to test extension loading." Today `quack` is the experimental remote-protocol core extension; the template demo is `waddle`. (§0)
- **The template docs are ahead of / behind the code in spots.** `docs/README.md` claims one function `waddle()` returning `Quack Jane 🐥`; the source has two functions and returns `"...........🦆 ..."`. Trust `src/`. (§1)
- **`ExtensionLoader` is the current API.** Copy-pasting older `DatabaseInstance`/`ExtensionUtil::RegisterFunction` examples will not compile against the current template. (§3)
- **Unsigned = explicit opt-in.** Locally-built `.duckdb_extension` files fail to load unless the connection was opened with `allow_unsigned_extensions` (CLI: `duckdb -unsigned`). Community and core extensions do not need this. (§6.1–6.2)
- **Binaries are version-locked.** An extension built for DuckDB v1.5.4 will not load into a different DuckDB version. Rebuild per DuckDB release; keep the submodule pin and the distribution workflow current. (§6.4)
- **vcpkg is pinned.** The docs pin a specific vcpkg commit (`ce613c4...`) — don't assume `HEAD` of vcpkg works. (§4.1)
- **Rust template is experimental.** `extension-template-rs` is explicitly marked experimental and "works with community extensions" is "coming soon" — don't build a production distribution path on it without checking its current status. (§7)

---

> **Relevance to core-x:** None of this is on the hot path of the out-of-core DuckDB → Arrow → Lance-on-R2 pipeline, which relies on **core** extensions (`httpfs`, `parquet`, `json`, `aws`) — see [07_httpfs_s3_r2.md](07_httpfs_s3_r2.md), [04_parquet.md](04_parquet.md), [10_core_extensions_catalog.md](10_core_extensions_catalog.md). The extension template matters to core-x only in the narrow case where a pipeline needs a **custom in-process C++/Rust scalar or table function** (e.g. a bespoke resolution-key normalizer that must run inside DuckDB's vectorized executor rather than in Python) that isn't expressible in SQL. If that need arises, note that a locally-built extension requires `allow_unsigned_extensions=true` on every connection that loads it, and its binary is pinned to one exact DuckDB version — a version bump forces a rebuild across the fleet. Do **not** confuse the `quack` core extension (remote DuckDB↔DuckDB protocol) with the template demo; the remote protocol is not part of the R2/Lance data plane.

---

## Appendix — verification log (fetched 2026-07-08)

| Claim | Source | Method |
|---|---|---|
| Current DuckDB stable = v1.5.4 (2026-06-17) | `duckdb/duckdb` releases | `gh api repos/duckdb/duckdb/releases/latest` |
| Template example fn = `waddle` (+`waddle_openssl_version`), returns `"...........🦆 "+name` | `extension-template/src/waddle_extension.cpp` | GitHub contents API, base64-decoded verbatim |
| `EXT_NAME=waddle`, ci-tools makefile include | `extension-template/Makefile` | contents API verbatim |
| Build cmds, artifact paths, unsigned load, community/custom distribution | `extension-template/docs/README.md` | contents API verbatim |
| `DUCKDB_CPP_EXTENSION_ENTRY` / `ExtensionLoader` entrypoint | `extension-template/src/waddle_extension.{cpp,hpp}` | contents API verbatim |
| vcpkg pinned commit `ce613c4...`, OpenSSL dep | `docs/README.md`, `vcpkg.json`, `CMakeLists.txt` | contents API verbatim |
| `quack` = remote client/server protocol, experimental v1.5.3 → stable v2.0.0 | `/docs/current/core_extensions/quack`, `duckdb/duckdb-quack` README (branch `v1.5-variegata`) | WebFetch + contents API |
| `quack_serve` / `CREATE SECRET (TYPE quack)` / `ATTACH 'quack:...'` | `duckdb/duckdb-quack` README | contents API verbatim |
| Rust template: C Extension API, `make configure/debug/release`, `rusty_echo`/`rusty_quack` | `extension-template-rs` README + `src/lib.rs` | WebFetch + contents API verbatim |
| `quack_stop`/`quack_identify`/`quack_query`/`quack_uri_parser`/`whoami()`/`DETACH`, port 9494, `application/duckdb` serialization, `httpfs_connection_caching` | `/docs/current/quack/overview` + `duckdb/duckdb-quack` source | WebFetch of overview page **and** GitHub code search (`quack_serve`=31, `quack_stop`=23, `quack_query`=16, `quack_identify`=2, `9494`=7 hits on `v1.5-variegata`) — CONFIRMED (§5) |
