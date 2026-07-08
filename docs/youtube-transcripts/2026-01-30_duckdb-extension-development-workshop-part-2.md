# DuckDB Extension Development Workshop — Part 2

**Speaker:** Query Farm founder (author of ~30 DuckDB extensions) — 2026-01-30 workshop.
**Scope:** Part 2 is the deep-code half — building a **table function** (`incremental_sequence`, a re-implementation of `range`), adding **multi-threading**, column **statistics**, and **publishing** to community extensions.

*Workshop transcript. Cleaned from an auto-generated transcript ("DuctTb"/"ductb" → DuckDB, "Gabber" → Gabor, "cardality" → cardinality, "Terra"/"mini ginga" → Tera/MiniJinja templating extensions, "stocastic" → Stochastic; wording lightly smoothed, meaning preserved.)*

---

## Q&A carryover from Part 1

- **`.mode line`** in the CLI expands wide rows (duckbox otherwise hides columns with `…`).
- **When can't you use unary/binary/generic executors?** When you want to **cache** something based on a **constant** parameter. Example: the `tera`/`minijinja` templating extensions — the template is fixed, so cache a compiled template once and reuse it with varying secondary parameters instead of recompiling per row. That needs a custom function rather than an executor. *(Query Farm extensions are open source — see Tera/MiniJinja.)*
- **Why does a binary executor exist if there's a generic one?** Optimizations differ per arity — e.g. **null handling** (if any input is null, the whole result is null) — plus possible compilation benefits of the non-generic template. *(Punt to DuckDB Labs / ask Mark.)*
- **C++ unit tests for your logic?** The speaker focuses on **SQL-based (integration) testing** inside the engine; DuckDB core supports C++ tests but they're not in the extension template (ask Carlo/Sam).

## Gotchas (a Claude-suggested slide)

You run in the **same process space** as DuckDB, so **memory safety is the biggest gotcha**. A wild pointer or memory overwrite surfaces as a "weird DuckDB bug" that Gabor can't reproduce — it's generally your fault if it's in extension code. Use **smart pointers** over raw pointer arithmetic. **Always use debug builds** — they verify data structures on return; release builds run fast and may hide bugs (no exceptions), but are faster because they skip the checks.

## Table functions

Table functions return **more than one output** (multiple columns, multiple rows). They signal completion by returning a **data chunk with cardinality zero** (zero rows); otherwise they can yield rows indefinitely, and DuckDB (streaming execution) keeps consuming them.

> **Streaming** = DuckDB yields rows as it processes them (2,048 at a time) rather than buffering an entire billion-row result before returning it.

We reimplement `range` as **`incremental_sequence(start, end)`** — counts from start to end, **exclusive of the end** (`[start, end)` notation: `(` on the left conceptually, `[`/`)` semantics). Note: `SELECT * FROM incremental_sequence(100, 110)` — a table function isn't catalog-integrated like `SELECT * FROM employees` (that would need the catalog/`ATTACH` API — too much for today). `git checkout step-4`.

### Registration

```cpp
loader.RegisterFunction(TableFunction(
    "incremental_sequence",
    {LogicalType::BIGINT, LogicalType::BIGINT}, // start, end
    IncrementalSequenceFunction,   // the execute/run callback
    IncrementalSequenceBind,       // bind callback
    IncrementalSequenceInitGlobal)); // global init callback
```

Unlike a scalar function, a table function has **no return type in the registration** — it has an **execute (run) callback** plus **bind** and **global init** callbacks. **Bind can run multiple times; init runs once** (at execution).

### Bind

The bind function receives inputs as **values** (not vectors — table functions get a single `Value` per parameter, each a logical type + scalar). `input.inputs[i].GetValue<int64_t>()` extracts them. It:

- Validates (e.g. throw a **BinderException** if end < start — surfaces to the user as a clean SQL error instead of a crash).
- Sets output **names** (vector of strings) and **return types** (vector of `LogicalType`) **by reference**. *(DuckDB keeps names and types as separate vectors rather than a tuple.)*
- Returns **bind data**: `make_uniq<IncrementalSequenceBindData>(start, end)`.

**Bind data** derives from `TableFunctionData` and stores the arguments (start, end as consts, set in the constructor). The `Copy`/`Equals` methods are boilerplate (omitted here — ask Labs why they're needed).

- *Why does bind exist?* It lets you **dynamically define the return schema** (and do polymorphism). The signature is mostly fixed at registration (`BIGINT, BIGINT`), but you can use `LogicalType::ANY` or **variadic** args for dynamically typed signatures — resolved in bind. The execute callback is **not** passed the original arguments, so bind is also where you capture them (into bind data) and tell DuckDB the memory layout to expect.

### Global init

Simpler — set up per-query state:

```cpp
state.current_value = bind_data.start;  // copy out of bind data
state.max_threads = 1;                  // control concurrency
```

`max_threads = 1` keeps a single thread so values return **in order**. Override `MaxThreads()` in the global state to control concurrency.

### Execute

```cpp
auto &bind = data.bind_data->Cast<IncrementalSequenceBindData>();   // safe cast
auto &state = data.global_state->Cast<...GlobalState>();
if (state.current_value > bind.end) { output.SetCardinality(0); return; } // done
idx_t count = min(remaining, STANDARD_VECTOR_SIZE);
output.SetCardinality(count);
// sequence vector: from current_value, count elements — cheaper than a flat vector
// (flat alternative: FlatVector::GetData<int64_t>(output.data[0]) then loop)
state.current_value += count;  // resume here next call
```

`SetCardinality` triggers allocation if needed (DuckDB knows the type from bind, so it allocates the right amount). Casting the wrong type in bind (e.g. declaring `VARCHAR` for int64 data) → an **address-sanitizer crash** from reading past the data. An `EXPLAIN ANALYZE` shows this can produce **100 billion rows in ~3 seconds**. `git checkout step-5`.

## Multi-threading (step 5)

Using threads generates sequences faster but **loses monotonic ordering** (values stay unique but may arrive out of order). Add a **local init** phase (per-thread) and restructure state:

- **Bind data:** still start + end.
- **Global state:** a **work queue** of sub-ranges + a **mutex**; a `GetWork()` helper that locks and pops an item; total-rows-returned tracking (for the progress bar); `MaxThreads()` now returns DuckDB's "as many as possible" value.
- **Local state:** current value, its chunk's start/end, a `has_work` flag, and the **thread ID** (so `incremental_sequence` can return the producing thread as a second column).
- **Work item:** a smaller chunk of the original sequence.

**Global init** now slices the range: `TaskScheduler::GetScheduler(context).NumberOfThreads()` returns `min(user set threads, CPU cores)` — that many slices pushed onto the queue.

**Execute** now: if the thread has no item, pull one from the queue (`GetWork()`); if none, `SetCardinality(0)` (this thread is done); otherwise produce the sequence vector **and** write the thread ID as column 2. *(Known small bug: the local state stores the thread ID at init, but the initializing thread isn't necessarily the executing thread — hand-waved.)*

**Concurrency notes:** DuckDB's scheduler is fully in control — you can't `pthread_create`; you give a **hint** via `max_threads`/`SET threads`, but DuckDB may launch fewer (it might use threads elsewhere, or the OS doesn't schedule them), and **work distribution is not uniform** — on an 8-core machine it might launch 5 threads, some grabbing a second chunk. Don't assume uniform per-thread work. You must guarantee only one thread touches shared state (the mutex).

**Performance:** ~100 billion rows took ~8 s single-threaded; scaling threads gives ~**1.9× / 3.6× / 4.9×** speedups.

## Progress & cardinality callbacks

- **Progress callback** — return 0–100 telling DuckDB how complete the query is; called often in the CLI for the ETA.
- **Cardinality callback** — how many total rows the function will produce. Combined with progress, DuckDB estimates completion time. Cardinality is also critical for **joins** — a bad estimate can put you on the wrong side of a hash join; an accurate one guarantees a good plan.

## Column statistics

Register an optional **statistics callback** (e.g. `IncrementalSequenceStatistics`) that describes each output column:

- **Numeric stats** (min/max), **null / not-null**, and (types permitting) string stats; distinct-value counts matter for join planning.
- For our sequence we know start/end, so we return exact min/max and "no nulls."

**Statistics are "covering" — DuckDB trusts them without re-verifying.** So `WHERE value = 50` when stats say min=100/max=200 means DuckDB **never calls the function** — empty result, work skipped. Same for `IS NULL`. Even two calls `a`, `b` with `WHERE a = b` get accurate filters/cardinality. **Implement statistics when you can** — big wins for little effort. (For a web-fetching table function you can't predict values, so return `unknown` — but still declare not-null if true.)

## Not covered (mentioned)

- **Projection pushdown** — a function producing 1,000 columns but only 2 selected should produce just those 2.
- **Filter pushdown / pruning** — push `WHERE` clauses into the function (e.g. `SELECT * FROM ps() WHERE user = 'rusty'` applies the filter upstream instead of yielding all processes).
- **Variadic args** (1..n) and **named arguments** (optional, `name := value` syntax, table-function-only).
- **Table in-out functions** — pipe a subquery's result through a function returning a table (e.g. `SELECT * FROM pii_mask(<subquery>)` for automatic masking — must be called explicitly, not auto-applied). Any function works in a **view**; `read_parquet`/`read_csv` are themselves functions, and a custom **file system** (e.g. a `sharepoint://` scheme) plugs into `read_csv` unchanged.

## Publishing to community extensions

Once done: in the DuckDB Labs **community-extensions** repo, add a YAML file `extensions/<name>/description.yml` with basic metadata following the existing format, plus your repo (`query-farm/workshop`) and the GitHub ref/version to publish. Open a PR → it's built and published, and anyone can `INSTALL <name> FROM community; LOAD <name>;`.

**Cross-platform is automatic** — the extension template's CI builds Linux (AMD64/ARM64, multiple glibcs), macOS, Windows, and WASM; exclude platforms you don't support. For platform-specific file access, use **DuckDB's file-system abstractions** rather than platform code.

**Adoption:** Query Farm has published 30 extensions; a recent build showed ~**76,000 DuckDB instances** loaded a Query Farm extension in one day (no user data kept — just load counts). The ecosystem grows in waves. Popular examples include **fuzzy string matching** and a **Stochastic** (statistical distributions) extension filling a gap the built-ins can't. Advice on what to build: "scratch your own itch," and put many fishing hooks in the water. Claude and other coding assistants have made extension creation much easier recently.

**Extension ecosystem report** shows growth from ~15 extensions at v1.0 to ~107 by v1.4.2, with the pace **accelerating** in the 1.4 branch. A gap today is **visibility** — people know DuckDB for Parquet/CSV but not the other ~150 extensions.

## Closing Q&A

- **Are extensions tied to DuckDB releases?** Yes — built per release. LTS (currently 1.4) plus the normal cadence; 1.5 soon → two extension branches going forward. (More on the future in Sam's afternoon talk.)
- **Community vs core:** community is like **PyPI** — anyone can publish, **no quality/warranty guarantees** (open source, read the license). Core extensions are authored/controlled by DuckDB Labs — a real difference.
- **Other languages:** you can write extensions in **Rust**, C#, or anything with a C-FFI boundary — less polished than the C++/C APIs today (see the afternoon talks).
- **Parser extensions:** to change query syntax (e.g. custom `AT <something>` beyond time/version), you need a **parser extension** that then re-triggers bind. The upcoming **PEG parser** will make parsing extensions much easier — on a parse error, your extension can be handed the unparseable query to attempt recovery.

Thanks for coming and listening for two hours. *[applause]*
