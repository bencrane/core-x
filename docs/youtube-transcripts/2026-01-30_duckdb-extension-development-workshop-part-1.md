# DuckDB Extension Development Workshop — Part 1

**Speaker:** Query Farm founder (author of ~30 DuckDB extensions; built the CLI query-ETA countdown/prediction and inspired the new v1.5 prompt) — 2026-01-30 workshop.
**Scope:** Part 1 is light on code — mental model of the engine + building a **scalar function** (`easter`). Part 2 goes deep on table functions and threading.

*Workshop transcript. Cleaned from an auto-generated transcript ("DUTDB"/"DuctTb"/"ductb" → DuckDB, "Gabber" → Gabor, "strruct" → struct, "cardality" → cardinality; wording lightly smoothed, meaning preserved. Interjections from the audience/DuckDB Labs staff summarized.)*

---

Good morning. I'm an informal speaker — let's make it two-way rather than me talking at you for two hours. Part 1 is pretty light on code; part 2 is deep on code. I'll bring you into the **mental model** I've built over two years of writing extensions — how I think about DuckDB internals and its classes. (I may have some of it wrong; the DuckDB Labs folks in the room can correct me.)

**About me:** I've written 30 extensions and started a little company, **Query Farm**, that just builds extensions. My most public thing in DuckDB is the **ETA counter** at the end of CLI queries — I brought in the countdown clock and the prediction algorithm, and inspired a bit of the new v1.5 prompt. Background in quant finance and a lot of Arrow work — Arrow, Python, DuckDB, the triangle of analytics.

## Lifecycle of a SQL query in the engine

1. **Parse** — SQL → abstract syntax tree (AST). *(We skip parser extensions today.)*
2. **Bind** — resolve the AST into actual types and functions to call. *(We cover this a lot.)*
3. **Optimize** — DuckDB rewrites the plan into an easier-to-execute form. *(We skip custom optimizers — maybe a future talk.)*
4. **init global** — called once per query at execution start (queries can be **rebound** multiple times, e.g. with the `ANY` type).
5. **init local** — runs per executor thread.
6. **Execute** — the execution phase.

We'll focus on **bind, init global, init local, and execute**.

## The repo

We build an extension together — a scalar function and a table function. Clone the repo (recursive clone — go easy on the Wi-Fi). It uses the **extension template** that Sam and Carlo wrote. Files are renamed from `quack` to `workshop` (there are case-sensitivity gotchas). Key files: `workshop_extension.cpp`/`.hpp`, a **SQL unit test** file, `CMakeLists.txt` (all DuckDB extensions use CMake), `vcpkg.json` (C++ dependencies), plus license and README. `git checkout step-1` reaches the first evolutionary step.

Build with `make debug` (uses Ninja if installed, which parallelizes CMake/compiler calls — ~3–4 min without ccache). The debug build statically links your extension into `./build/debug/duckdb` — no `LOAD` needed.

## Extension scaffolding

- An `extern "C"` block is the **hook**: when DuckDB loads an extension it `dlopen`s the shared library and must find the bootstrap symbol. Because C++ name-mangling makes symbols unpredictable, `extern "C"` exposes `duckdb_extension_entry` → `<workshop>` name + loader.
- Inside the `duckdb` namespace, the `WorkshopExtension::Load` function calls a static `LoadInternal`.
- `LoadInternal` is where you put your logic — add functions, add functionality, build out the extension.

## First function — `easter`

We build `easter(year)` → the date of Easter for a given year. Easter varies (equinoxes and lunar phases), so we use a Claude-provided anonymous Gregorian algorithm — we paste the logic in and build the **scaffolding** around it so DuckDB can call it.

**Vectorized execution:** DuckDB doesn't call your C/C++ function one row at a time — it hands you ~1,000–2,000 sets of parameters at once and expects that many results back. A **scalar function** means a **single output per set of inputs** (scalar *result*, not scalar execution).

## Mental model — types, values, vectors

Reading the DuckDB source cold is hard; start at **logical types**, then values, then vectors.

- **Logical type** — how DuckDB expresses the basic types in its execution model; physical storage is a lower level. Primitives (int64, int32, …) map to standard C++ types; date/timestamp are interpretations on top; varchars/blobs, hugeints (C++ doesn't support them nicely yet), bits, geometry. Logical types **compose** into nested types (struct, map, array, list, union). You use lots of logical types to declare your arguments' and results' types. *(Today: mostly dates and integers, some strings later.)*
  - *Q: Why "logical"?* Because composed types can be dynamic — a struct has fixed keys but its physical representation is individual vectors per member, so there's no direct 1:1 mapping between a logical type and a single C++ primitive vector.
- **Value** — a logical type associated with a single scalar value (e.g. `-30` as a BIGINT). A container pairing a logical type with the actual value.
- **Vector** — a collection of values sharing **one** logical type (stored once, not 2,000 times), plus:
  - a **validity mask** — a bit-packed mask marking which entries are null;
  - **buffer data** — e.g. an integer vector is a flat buffer of contiguous int64s;
  - **auxiliary data** — extra referenced memory. This is how **strings** are stored: `string_t` holds a **length** (UTF-8, can be binary), a **12-char prefix**, and a **pointer** to the data. Strings ≤12 chars are stored inline (CPU-cache-friendly; many strings are short, and comparisons within 12 chars beat a pointer-follow); longer strings live in auxiliary data.

### Vector layouts

DuckDB optimizes vector storage:

- **Flat vector** — data contiguous, one value after another, with a bit-packed validity mask.
- **Constant vector** — 2,000 identical values stored once (better cache performance).
- **Dictionary vector** — a child vector of unique values + indices (e.g. 0=apple, 1=banana) — good for low-cardinality vectors of large values like strings, avoiding duplication. (The strings live in auxiliary data; the child vector is part of the dictionary vector.) *(Speaker: in two years I've consumed dictionary vectors but never had to generate one.)*
- **Unified vector format** — an API call that lets you access any layout **as if** it were flat/uniform, so you don't have to special-case them. You can also produce these layouts in your extension for efficiency.

## Registering the scalar function

In `LoadInternal`, call `loader.RegisterFunction(...)` (the **extension loader** lets you interact with the DuckDB catalog):

```cpp
loader.RegisterFunction(ScalarFunction(
    "easter",              // function name
    {LogicalType::BIGINT}, // arg types (C++ vector) — year (int64)
    LogicalType::DATE,     // return type
    EasterFunction));      // the implementation
```

**All scalar functions share the signature** `(DataChunk &args, ExpressionState &state, Vector &result)` — args in, `state` (unused for now), and the `result` vector to write into. `git checkout step-2` has this.

Implementation uses a **UnaryExecutor** (a templated helper):

```cpp
UnaryExecutor::Execute<int64_t, date_t>(
    args.data[0], result, args.size(),
    [&](int64_t year) { /* compute Easter, return date_t */ });
```

- `int64_t` = input type (the year), `date_t` = return type.
- `args.size()` = number of elements in the input vector for this call.
- The `[&]` capture list captures nothing external — everything needed is passed in.
- The lambda runs **once per row** — up to the standard vector size (2,048) times per call. `SELECT easter(3000)` calls it with a single value; 20,000 rows → chunks of 2,048.

Build with `make debug`, run `./build/debug/duckdb`, then `SELECT easter(3000)`.

## Executors

DuckDB Labs provides **executor templates** so you don't hand-check validity masks or unpack vector types — write plain C++ and isolate DuckDB's vector handling:

- **Unary / Binary / Ternary / (…) / Generic** — named by argument count (Generic = n inputs → one output).
- They also do nice optimizations (e.g. handling a **constant** first argument without re-passing the same value).
- **Vector executors are only for scalar functions** — they make no sense in a table-function context.
- **Rule of thumb: use executors until you can't** (e.g. when you need to cache something per constant parameter — see Part 2).

## Documentation for your function

DuckDB lets you attach docs to a function so they show up in the `duckdb_functions()` table (~1,500 built-ins). Instead of the bare register call, create the function with **`ScalarFunctionInfo`** setting a **description, examples, categories, and parameter names**, then register it wrapped in the function info. Users of your extension can then discover how to call it. (`git checkout step-3`.) *(Gabor will likely build tools to extract this into community docs pages.)*

## Testing

The test lives under `test/sql/workshop` and uses **SQLLogicTest** ("SQLUnit" — heritage possibly from SQLite/TCL). Syntax:

- `statement error` — the statement should raise an error; give the statement and the expected error (e.g. calling `easter` before `require workshop` → "function doesn't exist").
- `require workshop` — loads the extension.
- `query I` — expect **one** column of an integer-ish type; give the query and expected value. (`I` = integer, `T` = string, etc. Files are **whitespace-separated** — in VS Code switch tabs→spaces or it complains about column counts.)

Run with `make test_debug`. Example `query II` returns two columns (year, Easter day), e.g. from a `range`.

*(End of Part 1 — break.)*
