# DuckDB Function Chaining: The Simpler SQL

**Format:** YouTube tutorial — 2024-08-24.
**Topic:** Making SQL queries easier to read using DuckDB's function chaining with the dot (`.`) operator.

*Video transcript. Cleaned from an auto-generated transcript ("duct DB" → DuckDB; wording lightly smoothed, meaning preserved.)*

---

This is a SQL query — it's quite hard to tell what it does, isn't it? You have to read from the middle and then go backwards through all the functions to figure out what it's doing. In this video I'll show you how to make the query easier to read using **DuckDB's function chaining with the dot operator**. We'll start with a simple example, see a gotcha when applying functions to literals, and then move to a more complicated example where we see how to work around a problem when a function can't be chained.

## The idea

Function chaining lets you take a function that takes `arg1, arg2, arg3` and rewrite it as `arg1.function(arg2, arg3)`, and so on. So it only works if the output of one function can be passed as the **first argument** to the next. Luckily, in DuckDB almost all functions are designed like that.

## Simple example

Launch DuckDB and write a query returning the name of this YouTube channel. To compute the length of that name, the traditional way is to go back to the beginning of the string, type the `length` function, and press enter — 20 characters. But instead:

```sql
-- traditional
SELECT length('...');

-- chained
SELECT '...'.length();
```

**Gotcha:** when using functions this way with **literals**, the literal needs to be in **parentheses**. Without the parens you get a syntax error — same with numbers or floats. Put the parens back and it's happy.

## Complicated example

Back to the query from the beginning. It takes numbers between 1 and 50 in steps of 5, computes the square root, raises it to the power of three, computes the log, casts to an integer (so we can compute the factorial), and returns the result.

Making it easier to read:

- Start with the `range` and **unnest** it.
- Select the number, then chain: `number.sqrt().pow(3).log()` — already a bit easier to read.
- **The stuck point:** for the cast, you can't write `.cast(... AS ...)` — that won't work. So you go back to the front: `CAST(... AS INTEGER).factorial()`. Not too bad.

If you're really into chaining and want it all the way through, create a **macro/function** to do the cast:

```sql
CREATE MACRO asint(x) AS CAST(x AS INTEGER);
```

This is specific — it only casts to integer, and I can't find a way to parameterize the target type of the cast, which is frustrating. But now use `asint`: delete the cast, put in `.asint()`, and the final chained function is:

```
number.sqrt().pow(3).log().asint().factorial()
```

Get the number, square root, power of three, logarithm, cast to integer, compute the factorial. Putting the original query and the chained one side by side, the chaining has made the query **much easier to understand**.

If you want to see more cool SQL innovations in DuckDB, check out the next video.
