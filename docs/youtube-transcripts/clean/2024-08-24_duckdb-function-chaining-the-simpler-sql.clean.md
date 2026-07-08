# DuckDB Function Chaining: The Simpler SQL — faithful transcript

*Faithful cleanup of the ASR transcript: every word of substance preserved; only filler removed and clear mistranscriptions corrected. Not editorialized. Verbatim source: `raw/2024-08-24_duckdb-function-chaining-the-simpler-sql.raw.txt`.*

**Format:** YouTube tutorial — 2024-08-24.  **Topic:** Making SQL queries easier to read using DuckDB's function chaining with the dot (`.`) operator.

---

This is a SQL query. It's quite hard to tell what it does, isn't it? You have to read from the middle and then go backwards through all the functions to figure out what it's doing. In this video I'm going to show you how to make the query easier to read using DuckDB's function chaining with the dot operator. We'll start with a simple example, we'll see a gotcha when applying functions to literals, and then we'll move on to a more complicated example where we'll see how to work around a problem when a function can't be chained. Let's get started.

So the idea of function chaining is that we can take a function which takes in arg one, arg two, and arg three, and rewrite that as arg one dot function and then arg two, arg three, and so on. And so what we learned from this definition is that it will only work if the output from one function can be passed in as the first argument to the next function. Now luckily for us, in DuckDB almost all the functions are designed like that.

Let's launch DuckDB, and then we're going to write a query that returns the name of this YouTube channel. Now let's say we want to compute the length of that name. So the traditional way will be, we'll go back to the beginning of the string, we'll type in the length function, and then we'll press enter, and you can see it comes back at 20 characters. But wouldn't it be nicer if, instead of doing that — let's go back and delete that length function from the beginning, and we'll call it on the end.

Now something to keep in mind when using these functions in this way with literals is the literal needs to be in parentheses. So let's delete those parens, and you can see we get back a syntax error, and it's exactly the same if you're working with numbers or floats, like you can see in this example. But put back in the paren and it's happy days.

Let's now go back to that query that we saw right at the beginning. We'll just work through what it does. So we're basically taking numbers between one and 50 in steps of five, we're then computing the square root of a number, putting it to the power of three, computing the log, we're then casting it to an integer so that we can compute the factorial of the number, and then we get our result. Let's see if we can make that a bit easier to read.

So we're going to start by getting the range and we'll unnest it, so that's the first change we can do, unnest afterwards. Then we're going to select the number, and then we're going to call number, square root, to the power of three, and log. And so you can see already it's a little bit easier to read. But now we get a bit stuck, because for the cast we can't call cast like as dot cast — there's no, that's not going to work. And so we kind of need to go back to the beginning and say cast and then as integer and then dot factorial, and we get our result. And it's not too bad.

But what happens if we're really into this chaining and we want to keep it chaining all the way through? So what we could do is we could create ourselves a macro or a function, asint, and do the cast in there. Now this is obviously quite specific, because it only works if you want to cast something as an integer. I can't find a function where you can parameterize the casting that you want to do, which is kind of frustrating. But let's now use the asint. So we're going to go back, we'll delete the cast, we'll put in the asint, and so now you can see this is our final function. So we got the number, we do the square root, the power of three, get the logarithm, cast it to an integer, compute the factorial.

And if we then put the initial query and this one side by side on the screen, I think the chaining has made this query much easier to understand. If you want to see more cool SQL innovations in DuckDB, you'll want to check out this video next.
