# Data Pipelines Generated With AI Keep Breaking? Here's Why

**Published:** 2026-06-25.
**Source:** YouTube — <https://www.youtube.com/watch?v=E_OO8HuNjaE&list=PLIYcNkSjh-0wlrFUE2VvQilLU2aBPns0K&index=1>

*Video transcript. Cleaned from an auto-generated transcript ("Moderndesk"/"Actimize"/"Mod AI" → MotherDuck; "the dive" → the dviz; wording lightly smoothed, meaning preserved).*

---

Writing a data pipeline with AI has never been easier. You type a prompt and a minute later something that runs is there. But a pipeline that *runs* and a pipeline that is *correct* are two completely different things. This pipeline is green, it ran — and the number is wrong.

I'm going to tell you four actions to turn a lucky one-shot prompt into a pipeline you can actually trust. The tips are tool-agnostic. I'll use a coding agent (Claude Code), DuckDB, and MotherDuck, but map it to whatever your stack is — your database, Postgres, dbt, whatever is your jam.

## Why AI-generated data pipelines break

Two things work against you:

1. **AI is non-deterministic** — ask twice and you get two different implementations. And that it *runs* doesn't tell you which one is correct.
2. **AI cannot see your data** — so it guesses your schema, your nulls, what counts as duplicates. Those guesses are basically silent bugs. No error, just plausible wrong numbers.

## Tip 1 — Foundation still matters

The old data engineering discipline did not disappear. It matters *more* now that AI can generate, so fast, code it's entirely sure is correct.

Example: a simple prompt that loads external NOAA climatology data — get the maximum temperature for a US station from the public S3 bucket (Parquet), load it into MotherDuck, and report the average and max temperature. With no MCP connected, inspecting the resulting pipeline: it's really simple — one DuckDB query — but roughly everything is hardcoded.

- The data is clearly **partitioned per year**, but the pipeline doesn't expose that as a parameter, so you can't load or reload a specific year/partition.
- It takes the temperature value as-is. Looking at the data, the average comes out to ~179 — really, really warm. The reason: **temperature is in tenths of degrees**, so you have to divide by 10.
- The source provides a **data-quality flag** on readings, and we don't exclude the flagged ones. That's why you get weird temperature results sometimes — flagged by the source, but inserted into our data set anyway. Not good — but the pipeline is still green.

**Fundamentals to put in the prompt:** parameters to inject for flexibility; make the pipeline idempotent; focus on incremental load instead of a full snapshot every time; and first inspect the data so you can spot the gotchas.

## Tip 2 — Give your AI eyes on your data

Use any MCP available for your database (pretty common these days). Using the **MotherDuck MCP** here to read the data from S3 and inspect it: prompt "use the MotherDuck MCP to inspect the data." It runs several queries and already identifies that the schema shows **data in tenths of a degree**, that the layout is **partitioned by year and by element**, and that there's the **Q flag** discussed earlier.

With access to the data plus the data-engineering fundamentals, the result is a robust pipeline. What we asked for:

- Values as **parameters** — e.g. output table and the **year**, so we can reprocess a given year.
- Convert obvious units — put the value in **degrees instead of tenths**.
- **Drop rows where the source quality flag is bad.**
- Make the pipeline **idempotent** — run it multiple times, same result, no duplicates — using the classic strategy: delete existing data within that time range (that year), then reinsert.

Now that the AI has eyes on the data, you can ask it to create simple **unit tests**. It's important to have an engine you can run locally or in CI without depending too much on the cloud — DuckDB does this. A simple test runs in under a second. Because it inspected the data first, it can **mock realistic look-alike data**, and it puts tests around the division, the temperature, the duplicate stations, and so forth.

## Tip 3 — Make the goal a contract, not a prompt

The hardest but one of the most important tips. The idea: you don't judge the *code*, you judge the *outputs* against a goal you set up front.

For my pipeline, the contract specifies:

- A specific table, **QA-based, with a daily maximum temperature**.
- The value is in **tenths of a degree**; parse the dates; etc.
- The important thing: **same-station mean Tmax per year.** To measure a trend you must hold the same station set fixed. Multiple stations get added over time — if I look at the United States and want a trend, I'd be biased by new stations added (e.g. in cold areas). So I only look at the **initial stations still present today**.

This kind of contract — how you want the data to look, plus specific checks that must pass — you can create with AI and iterate against the data set.

For fast agent-loop testing beyond local unit tests, I use **MotherDuck Flight** as a remote Python compute. Through the MotherDuck MCP, the AI can deploy the Python pipeline, run it, see if it fails or succeeds, get the logs, and try again until the goal is met. Again, the goal is **not** that the pipeline is green — it's the specific contract.

**Bonus:** I asked it to create a **dviz** (data visualization in pure JavaScript). Through the MotherDuck MCP, the AI generates the JavaScript and deploys it based on the data we just ingested. It's nice to visualize — you'll spot weird things faster in a chart than in a pure pipeline, even if your job is just to ingest and provide clean data sets.

## Tip 4 — Package it

All these extra instructions — adding parameters, making it idempotent, get eyes on the data before writing any transform — build them into a simple markdown file, a.k.a. a **skill file**. Put the name, a description, and the rules we just defined. Then, specifically for MotherDuck AI: first build a Flight, add unit tests, and so forth. Package whatever needs to be reused for the next pipeline.

For inspiration on agent skills for data pipelines, look at the **MotherDuck AI DB agent-skills repository** — tons of skills for data work around MotherDuck, reusable for other Duck-stack setups too.

## In short

- Foundation still applies.
- Give the AI eyes on your real data.
- Make the goal a contract instead of a prompt.
- Package it so it happens every time you write a data pipeline with AI.

Stay cool, and I'll see you in the next one.
