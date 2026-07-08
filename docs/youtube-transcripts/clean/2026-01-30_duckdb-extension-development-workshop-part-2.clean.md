# DuckDB Extension Development Workshop — Part 2 — faithful transcript

*Faithful cleanup of the ASR transcript: every word of substance preserved; only filler removed and clear mistranscriptions corrected. Not editorialized. Verbatim source: `raw/2026-01-30_duckdb-extension-development-workshop-part-2.raw.txt`.*

**Published:** 2026-01-30.  **Source:** DuckDB Extension Development Workshop (Part 2).

---

Questions and answers over what we've covered so far, because you guys have had a couple minutes to look at it. Does anybody have more questions that they need clarified? Yes.

>> Let's use the microphone so that it gets captured in the recording.

>> Oh, sorry. Which mode were you using in the DuckDB binary when we were querying?

>> The debug mode?

>> But you do dot mode something.

>> Line.

>> Line.

>> This one is query. This one — you mean like here? Oh, I'll jump into VS Code.

>> When you were looking at the description of the function.

>> Oh, yes. That dot dot dot — dot mode line, dot mode line. Yeah, because duckbox, if you have many more columns, it does dot dot dot and hides them from you.

>> Okay, thank you.

>> Who's that? Somebody else has a question.

>> You mentioned you normally use the unary, binary executors and the generic executors. What are use cases that you can't use them, or are you going to get into that later?

>> Well, one use case would be when I have a parameter that is constant and I want to cache something based on that parameter. So I have this extension called Tera and MiniJinja, which are template functions, and a lot of times when someone's going to call me as a scalar function to include a template, their template is fixed. So I want to determine that value does not change, do a cached compilation of that template into template A, and then just use that cached template A with varying secondary parameters for that call. So it allows me to prevent compiling that template every time for the execution. So that's when I'd have to use a custom executor, or a custom function rather than an executor. There are probably more examples, but — yeah, my extensions are open source. So you can go into Query Farm and take a look at Tera or MiniJinja and see that.

>> All right. Thanks.

>> You're welcome. All right. Any more questions?

>> Yeah, just a small question. Why does a binary executor exist? You have a unary executor. Why do you need binary at all?

>> And actually — I didn't, but I think this was how it grew up into different optimizations that can happen for unary, binary, ternary executors, because sometimes with scalar functions you have this thing called null handling. So if there are any nulls of the input, the one thing those executors can do is say, oh, the third argument is — the second argument is null, the whole result has to be null. So that's a part of what the executor wrapper does. And I also think there may be some compilation benefit for the non-generic template of how it's executed. But that should really just be a Labs question. I'm going to punt on that one. So if anyone wants to take that, go ahead. No, no, no. [laughter]

>> Ask Mark. Yeah, if you see Mark around, he'll tell you.

>> Yes.

>> Oh, what? In the test, I couldn't provide — so this is query I, and you mentioned I I for two columns. So then what if I want to add a third query that would also be query I but then a different query?

>> Yeah. So you would have a new line on the bottom and then you could do query I I, and I I think is integer, A is for ASCII string, T — I'm not sure what those letters really mean. I've cargo-culted my SQL unit test knowledge. But yeah, it's just query I I would be for three columns, query I I I I for four columns, those types of things. But I'd be happy to attend a session about SQL unit testing in the next DuckCon.

>> Noted.

>> All right. No further questions. One more.

>> So in this case, it's a simple logic of Easter. But let's say I am doing some custom stuff and I would like also some unit tests for my C++ logic.

>> Okay.

>> Is that a common thing to do, or —

>> In my extension building I've mostly focused on SQL-based testing. There are some hooks for C++ testing, but I haven't seen that in the extension template. But I'd have to talk with Carlo and Sam to see how that's built in. But I know DuckDB itself supports C++ tests, but I haven't written any. But there might be someone in the room that has.

>> So these would be like exec — integration tests.

>> Correct. These are when you're testing here — it's complete integration testing because you're really inside the DuckDB engine.

>> Thank you.

>> All right. So seeing no further questions, let's continue. Great. This is a review of some gotchas. I ran my talk through Claude and it was like, hey, how can I improve it, and it was like, add a slide about where problems are. So this is a Claude-powered slide and reviewing it. When you're writing C++ code, you have to realize you're running in the same process space as DuckDB. So memory safety is the biggest gotcha in extension creation. So if you have a pointer that goes crazy, or you're overwriting memory somewhere, the exception will show up and it'll be like, "Hey, why does DuckDB have this weird bug, and I'm filing it, and Gabor says I can't reproduce it." It's generally your fault if it's in the extension code, because we're in the same process. So use the tools that C++ gives you with having smart pointers rather than regular pointer arithmetic if possible. Always use the debug builds, because it does a lot more verification of the data structures themselves when you return them. Whereas if you use a release build, that's just going to go as fast as possible, and if you have a bug, you might not see it because the exceptions won't be there. But debug builds are slower because they're doing those additional checks. And you guys can see the other things here, but we'll get into it.

But now I want to transition you into table functions. So we did scalar functions for our first half. Table functions are going to be more C++ code because there's a bigger, wider API for them. So be ready. Hopefully we're more awake than we were about an hour ago. And table functions can return more than one output value than a scalar function. So they can return multiple columns. And the only way they know they're done is when a table function returns a data chunk that has a cardinality of zero, meaning it has zero rows. That table function is complete. Otherwise, it can just keep yielding an infinite number of rows back to DuckDB. And since DuckDB executes queries in a streaming manner, it will just continue to consume those rows over and over and over again. We're also going to now touch on the bind and global init callbacks, because that's what you need for table functions to tell them, hey, these are the columns, and these are the names and the types of columns I'm going to return. So please update to step four in your code to follow along.

>> Streaming means, when DuckDB runs a select from a table, it's not going to buffer all those rows into memory before it gives you the result. It will yield rows as it processes them through the query execution. So it may yield you the first rows and then the next 2,048 rows and the next 2,048 rows in a streaming manner, rather than say buffering the entire one billion result set into memory and then giving it to you all at once.

>> We are starting to — we're not going to give —

>> We are — you'll see, we are going to get there. Yes. Yes, they can. But we are going to demonstrate multiple threads in this workshop, but they can also run in parallel processes in a different way, but we'll get into that. There was one more question, one through the other ones —

>> Branches.

>> I'm sorry. Consistency — I will get, I will be better at this. It's my first time through. So we're beta testing the presentation model, but if I give this talk again, they will all be consistently probably tags. If you guys are ready, we'll keep going.

So this is the example table function we're going to do. We're actually going to reimplement range, because I didn't want to break our brains with too much stuff today. Range is a function that just says count from starting value to ending value. And we're just going to call it incremental sequence, because I couldn't think of a better name than range. But incremental sequence is what we're going to build. And we'll give it, say, 100 to 110. It's exclusive of the last value, which is the tricky part of this function. So it has the parenthesis on the left but the square bracket on the right, if you're going to write it in the range-based notation. Any questions so far?

So we're not going to implement select star from a table. A table function isn't integrated with a catalog in the sense of like select star from employees. But these table functions allow us to do select star from this function, passing it arguments. And to return from a table, we would have to implement the catalog API with the attach syntax. And that was just too much for this talk. But next time we could get into that.

Keep going. So we're going to start by going back into our good old load internal function. And we had Easter defined in there. And I'm just going to build on top of that. We're going to add a new function called table function incremental sequence, which — we'll step through it. There's the name of the function again. This is like a scalar function where we had Easter before. Now it's incremental sequence. Then we have our arguments. This is going to take two int64 values. A big int for start, big int for the end. We're going to have a function that's going to produce our rows. Notice how, like a scalar function, in this call would be, here's the output type. Table functions don't roll like that. They have a function which — I'm not quite sure what this function is called, but I'm calling it like the execute function. The run function would be another name for it. And these are all callbacks. And this is — we're going to have a bind function. And bind is that bind phase of the query that runs after the parse to tell you, these are the columns that we're going to return, and these are the values. And then we're going to have global init. And that will be like when we start to run this function, here's an init function to set up some values. Bind can run multiple times. Init runs only once, because it executes when the query executes. And finally, we'll just call register at the end.

I told you the code's going to get heavy. It's probably best to follow along on your laptop, or you look up on the screen. But this is bind function time. We're going to start through it. So we have our start and our end value. And these are just grabbed as two vectors. And we're going to say input.inputs is an array of values. And now they're not vectors, they're values. I'm sorry, I misspoke. They're values. Remember how I said in the original part, we have values that have a logical type and then a scalar value. Table functions aren't passed vectors of their parameters. They're only passed a single value. So they're values. Get value is a templatized function where you can give it the primary type, and it'll just return that for you. And we're going to throw an exception. This is a case of like, if your end value is greater than your start value — I mean, if that's not the case, we're going to say, hey, you can't do that. We're going to raise an exception that'll come back to the user as a nice, like, hey, this is an exception in your SQL, you need to adjust your values that you're passing to me, rather than crashing the program.

And finally, we have the names and return types. Names are the names of the columns that are going to be returned back, and then return types are the return types of those columns. Why this is not a tuple, where they're — because these two things are associated — is, it seems like DuckDB doesn't really like to use tuples together for this. So they want to keep them separated. So they're just — names is one vector and then return types is another vector of logical types. So return types is logical types, names are just strings. And they're passed in as references into the bind function. So you're manipulating them by reference of the caller. And then finally, we're going to return a bind data, make a unique value of an incremental sequence bind data structure, a bind data class, with a start value and the end value using the constructor — and we'll get into that, but that's up here.

So in the bind data we need to store these arguments, our start value and our end value. And to do that, we need to derive from function data class, for the function data class to say this is the bind data associated with our table function, for our start value and end value. And a lot of this is boilerplate, because the two members are right here, start value and end value, and they're constants. We put them into the constructor, and then these two other methods, copy and equals, I just omitted because they're saying if we wanted to copy this class, or if we wanted to compare it, we could do that. Why we need those methods, that's a question for the Labs guys or girls. In that case — this is a lot, it's more than this morning. Are we okay, are we hanging in there? I see some yeses tentatively. Okay.

>> Yes. Uh-huh.

>> You would get a compilation error, because the value that you're pushing — but the vector is already typed. So you see, like the vector here, it says vector logical type, vector string.

>> Yeah. These are not DuckDB vectors. These are C++ vectors.

>> So they're not — they're not like what we discussed earlier. These are just your normal standard C++ vectors.

>> I mean, oh, like logical type var.

>> Yeah. Exactly.

>> Okay. I want to do this to see what happens next.

>> Okay. What will happen? Well, you could do that, and then as we get into the implementation of the function, if the functions are still implemented as they are, you'll likely get a crash, because the size of the memory layout will be different. So we're going to assume that there are int64 values for these two ranges. But if you say it's a varchar, it's going to say, oh, I'm expecting that to be a string type, and it's going to try to read beyond the end of the data that we've defined. And you'll get an address sanitizer exception most likely.

>> Purpose of the bind operation.

>> It allows you to dynamically define not only — mostly the return value, not the signature. The signature is mostly defined when you do the registration in the previous screen here of the function. You see how we have the arguments here of big int and big int. That's the signature. But you could also have logical type any, which allows you to have polymorphism, or you can have variable arguments like var — I'm using my pronunciation, I don't know if I'm saying it right here for you guys — but variable arguments, which allow you to do dynamically typed function signatures with the bind. Yes, that's the real benefit of that bind function. It allows you to do, as my CS professors would say, polymorphism, sort of maybe, but not encapsulation. These are really old cobwebs.

So you guys got the bind function. Now we're going to get into global init. And this is a lot simpler. When we global init, we just want to say our starting value is the start value of the interval that we want to calculate. But we need to save that as well. So we want to keep this state to be saved. So we're going to say, have our callback for global init, we're going to say our state current value equals the start value from our bind data when we start to execute, because the bind data we really don't want to reference too much. You could do it in that way, but I did a global, and it'll just be nicer, because you'll see where we get to on the next stage. Here's the global function state, the global state, and it has the current value, but it also has this thing called max threads. And this is how you can control your concurrency of your table function. So right now I just want to have one thread. I don't want to have eight different separate workers all executing my table function to produce results, because I want the values to return in order, and if I just have one thread, I know I'm starting at the start and I will go all the way to the end, and we'll get there. So this in general — if you have a table function, you want to control concurrency, you override the max threads method in the global state. Any questions?

>> Sure.

>> Yeah. So we have the name of the function, we have the arguments, and then these are three different callbacks where it's the executing function, the bind function, and the global init function.

>> I agree with you. The reason why it has to be complicated is, it's a lot of boilerplate to store the arguments. The function, when we execute the sequence function, doesn't get passed the arguments from the bind phase. Do you understand? So when I say incremental sequence, start like 100 to 200, that 100 to 200 — how do I pass that down to the execute callback? It's not actually sent there. And also the bind phase needs to tell DuckDB what are the return values of the table function, and I can't really do that in the execute call, because by the time I get there, I need to know what the memory layout is to expect to produce. So that's why we have to pay the cost at this phase to do those two decisions. But you're right, there is complexity here that is larger than the other phases. Do I do a bad job of answering? Probably.

>> Well, a sequence in this case — we're writing a function that will produce these rows from like 100 to 109.

>> Yeah, this could be also list employees. It could be, what are the current processes running on my computer? It could be anything a table function can return. It's just a multicolumn, multirow function, as opposed to a scalar function that has multiple inputs but a single output. That is absolutely great. That is correct. As for query planning — there's more to query planning than that, but you are correct that that is the right understanding. There's also query planning for how many rows will this function return, kind of like the canonical estimate of the function. But you're right in that understanding. Let's roll along here. Go back.

>> Can I ask a question?

>> Yes, you may. Sorry.

>> It was for — and I'm just —

>> You will see, we're going to get into that, and we're actually going to make sequence into a multi-threaded sequence generator as we go on. Were there other questions? Okay.

So now we're going to go into the execute function. This is the function that's run to actually produce the rows, and we'll step through it. These two lines here are grabbing the bind data and global state data and casting them to the derived types, because when they're — how DuckDB passes them in as the generic type, and we're casting them to the subclasses, is the right way to say it. So we're just using a safe casting mechanism, rather than — if you're — I grew up in C rather than C++. So I would always be like, open parenthesis, type name, pointer, cast, we're done. That's not so safe anymore. That's not the way to do it. So I've really learned a lot about C++ by using DuckDB and developing inside of it.

So secondarily, then, inside of this we say, if the current value is greater than the end value, we stop. We say output set cardinality zero, meaning there are no rows on our output. Stop doing anything. Don't call me anymore. That's why we have to say set cardinality to zero. But otherwise, we grab the remaining number of rows of like between the end to where we are, and then we grab the row count of the chunk, because if we have like a 10,000-row sequence, we can only return 2,048 of them in this call, and we set the cardinality. And then we say, make a sequence vector, which is a really cool vector compared to say a flat vector. We can say go from this value to this value using this increment, using sequence. But if you wanted to do the flat vector way, you can say get data as int64, and then populate it in a loop from zero to the number of rows in this particular function. And you just — result I is where the output value is in memory of these sequence values. And then that's it for calculating the sequences. You guys all good? This is the simplest way I can make a table function be, I think. But if it's simpler, let me know if you have better ideas.

>> So you mentioned that for the values, there's like this string that tells you which are null, for the arguments.

>> Yes, the selection —

>> Return. How would that work?

>> That would work — you would change the result. You would say result set valid or set invalid, I believe, is a function call, and then you give the offset in the result vector of which particular offset in the result vector is null, I believe. But I would have to look at my other extensions to find out, but I'm pretty sure that's right.

>> Yes. To me, where the global state is getting updated — at the last row of this function, it says global state current value plus equals row count.

>> Sorry, I didn't highlight that, but I will next time. So that's the end of — we're saying, we just gave you this chunk of values, and the next time a call may go on from there.

All right. So if you did an explain analyze, you're going to see that this function could return 100 billion rows in about 3 seconds, which is pretty fast, pretty good. Counting to 100 billion is cool. I don't know if anybody's going to check me, but it works. Go to step five, please, on your git tags.

>> Yes, I can. Yep. Uh-huh. Let me help you out. So in the bind function we have the return values, right, and we said we're going to return int64. So when I get called by DuckDB, it has the data chunk already kind of here with output, and then I can say set cardinality — the line above this — we'll say how many rows I need to be in there, and I believe that will trigger allocation if necessary to store those values, so that you actually don't need to mess with it. And since it knows the type, it automatically knows how to allocate that much memory in the resulting vectors.

>> Yeah, because you see this auto reference thing here. So that's just going to say, like, it's just a reference to the memory, and I believe it's just like an int64 pointer is the actual type right there. I'm proud of you guys for sticking with me, because C++ on slides is not easy.

>> Yes. Unified vector is mostly used for reading —

>> This is my understanding, because I think, like, if you do unified, you're basically casting whatever the vector is, you're putting it into that unified representation. I haven't really used unified vector for writing, but the unified format allows you to go to like, what index is element n of this vector. And if it's a constant, it'll do the right thing. If it's a dictionary, it'll do the right thing. But when you're writing, I'm pretty sure it's always going to be generally a flat vector, but I know you can write other types, but I don't know. We might have to follow up on that. Sorry to muddle your answer on that, but I think it's okay. But the thing is, the data will never be huge, because DuckDB only works in 2,048-row chunks. So you're always going to know it's not going to be 10,000 rows. It's not going to be 10 million elements there on the DuckDB side, on the vector.

>> It's sequence vector is what I went with, because I am trying to be cognizant of the number of rows of lines of code on the slide. But flat vector, I just did it here to show you, if you didn't want to use sequence, you can use the flat vector.

All right, so now we went to step five, and here's where we're going to go crazy. If we start using threads, we can go faster to generate our sequences, but it also means we're going to lose the ability to say the sequence is monotonically increasing. So the values of a sequence will all still be unique, but they may not be in order. And to do this, I thought it's worth to demonstrate it, because I'm going to show you how the parallelism works. And I'm going to show you how we have to work around — sometimes we get threads, sometimes we don't get threads, sometimes the work assignments are unfair. We'll get into it. But we're going to actually add a new phase here called local init, which will be initializing for each thread.

Here's where I said it's getting complicated. We need to understand what we are putting on the structures for the threading. So our bind data is still going to have our start value and end value. Our global state is going to now have a work queue, where we're going to put portions of the sequence range into a queue where threads can pull them off and produce them, with a mutex for locking, because we don't want to have the same thread get the same thing more than once, or the same thing when they all request it. Our local state will have our current value and our start and end, and if we have work or not. And finally, our work item will just be a smaller chunk of the original sequence. You guys all right with that? Cool.

Here's our global state changes. Here's our original global state, where it's just like, here's our current value, and we're going to have to build it up into a bunch of chunkier things where now we're going to have a queue. And these queue details don't really matter. These are just like C++ queues. There's no DuckDB-specific stuff here. Here's a function that grabs the mutex and tries to grab an item off the work. And the work queue again is just portions of the original sequence. Here's some tracking for total rows returned. I like progress bars to work in my DuckDB functions. So if we track how many values we've produced out of the sequence, we can get that progress bar working, where it'll just count up to completion of the query. We changed our constructor to just track the total rows. And finally, our max threads, rather than being one like we had before, is now the total — there's a static value that DuckDB uses for use as many threads as possible. And that's what I basically said, because we're going to arbitrate the work that way.

Now we'll have a queue, like I said, global init will have a queue to execute. That was the old version of global init. This is the new version of global init. Again, we're casting our bind things. We're creating a new state here for the start and the end, the number of threads. This is telling me how many threads will DuckDB already have. If you say set threads equals one, we're only going to put one range on the work. But if it tells me you have eight cores, this value of TaskScheduler get scheduler number of threads will say eight — it's whatever the maximum value of the CPU cores with the minimum value of what the user has specified of set threads to, gives is this value of number of threads. And that's just telling me how many slices I want to slice up the range into as I put it onto the queue. And then I'm going to loop here, slice up the range, and push it onto this state work, and push it to go on. I see head nods, which is good.

Here's our local state. When we get a work item, we need to store what we're working on. So local state is local to the thread. We have current value, where we are, because again, the chunk we give to a thread could be bigger than 2,048. So we need to resume where we are within the status of that sequence chunk. Here's our constructor, where we're just grabbing some initialization around. We're storing the thread ID, because you'll see I'm now having sequence return both the value and the thread ID as a possible column. So we can tell which thread produced these values. And has work is just an indicator: does this thread have a chunk of the sequence to work on right now? And here's the constructor. It's just a simple, make me this object. The default constructor is called there.

And finally, we're getting into execute, which is going to be — this is the original execute that we had before. The execute got a little bit longer. I'm sorry. This is the problem with multithreading. Global and local state. Now we're going to say get new work. If we don't have an item, we're going to go to that queue that's sitting in the global structure. Grab the work. If not, we're going to say we're done with producing values on this particular thread with that set cardinality zero, and that will end our executor. Otherwise, we'll store it on our local state, like these two lines here. And here's where we're doing the same, producing a sequence vector, but we're doing the second thing. Output — this new line here is saying store the thread ID of the values we're producing as the second column of this function. Any questions on this? I feel like I've tried to talk through it as much as possible. But the nice thing here —

>> No, I don't really know. The DuckDB scheduler can do whatever it decides to do. I'm not in control of the pthread create or anything else like that. It really does it all.

>> Yes. So you have to guarantee that only one thread is accessing state.

>> That is correct. That would be a problem if that was not true. And what I did realize was a bug here would be, this local state is storing the thread ID, but the initializer of the local state does not also have to be the same thread that is executing. So that is a little bit of a bug there, but I'm handwaving that away, because we'll get there.

So we'll skip along here. Progress and cardinality callbacks — in the aspect of time, I'm just going to skip past this, but progress callbacks allow you to return a value between zero and 100 that tells DuckDB how complete you are in processing the query. And it will just be called pretty often in the CLI to say, how far along are we. It's really good for estimating those ETAs. And then cardinality is a callback that says how many total rows are you going to produce as a table function. And if you can tell me that plus your progress, I can do an estimate of when you're going to be done. Cardinality is also important for joins. So if your cardinality estimate is not good, you may be put on the wrong side of, say, a hash join, whereas if it's really accurate, you're always going to be guaranteed to get a nice query plan.

And then bind changes. Here we're having the value is the first column and thread ID is the second column. So the first column is a big int, the second column is an unsigned big int for the thread ID. And this is just changing the bind function to have those two columns.

Now here's where you get the performance. So if one thread — we were just doing for, I think it was for like a 100 billion rows, took about eight seconds. But as you increase threads, you'll get 1.9, 3.6, 4.9 times faster, because we chunked up the work and those other threads can execute. This should not surprise you all, because CPUs are cool, and everybody has a ton of different cores now. And it works well. And here's where I wanted to show you that even if you have a thread, the work assignments don't actually have to be uniform. So you can see these three threads got twice the work done on them than the original threads did. Even though the sequence was evenly chopped up in the work queue, DuckDB may not launch eight threads. Like, my machine has eight different cores. So it may not launch eight threads. In this case, it only launched five, and those threads were so fast, they got done with their first chunk of work and then they got a second chunk, and that was it. So don't assume that your threads are going to have a uniform distribution of work when they come across the executor.

And then we can tell about column statistics, which is even cooler. So when you have a DuckDB table function, you can have this additional callback that says, hey, here's statistics about the columns of my function. It will help the query planner do better assumptions and better filtering about your queries. So in this case, I have statistics here that say, when we're building a sequence, I know what the minimum value will be and the maximum value will be of the column, and I also know all the values won't be null — and that'll be important in just a second. This is for the thread stuff. I just know there could be a total number of threads, a total number of distinct values, and a unique value here.

But here's where it's cool. Statistics. Now that we implement the statistics callback, these queries don't actually run the function. So normally we'd have to run from like 100 to 200. But if I say where value equals 50, and those statistics say the min value is 100 and the max value is 200, DuckDB will never call our function. It'll just say, yo, man, there's nothing to do here. Empty result. The statistics say the value will never be within the range, we don't have to execute you at all. So it's a nice way to skip work. Same thing for is null. You can do those same types of things. And if you want to do something really crazy where you do two calls of your same function, like a and b here, and you want to say where a equals b, it will look at the statistics and determine the filters that it needs to apply to those functions. And you can see that by doing that, the estimates of the cardinality really become much more accurate. So please implement statistics for your functions if possible, because great things happen that you really don't have to work too hard to have the benefit of.

>> Statistics are a callback on the function, called incremental sequence statistics, and — you know how we registered the function. I'll show you in the real code here. One second. Git checkout step five. Let's go down here. Do I get there? One second. Live coding problems here. There we go. So statistics are registered as a callback right here. It's just another optional callback to your table function that allows you to say, hey, if you want to grab statistics about the columns that I'm going to return from this function, please call this function. They're really specific for the types of the columns. So we have numeric statistics where it's like min and max values. We have null or not null, like can the column contain nulls, can the column not contain nulls. The DuckDB guys will probably know there's more stuff there, but there's string statistics for like min and max values for strings. It really depends on what the DuckDB team wants to give you for statistical stuff. So soon they might have, say, a more — number of distinct values is really important for join planning. But how that's represented, I'm not sure. I don't think there's anything like Stochastic distributional sampling — I don't think there are those types of statistics in there.

>> Yes.

>> Yes. You implement this function right here, incremental sequence statistics, just like this. So I say like, oh, if it's column index zero, that means it's our sequence value. Here's our numeric stats, because it's a numeric type, it's a big int. Here's our min, here's our max, and it has no nulls. I'm going to return the result.

>> Yes.

>> How do you — threads?

>> That's on the global init data, and you have the max threads callback. But if you're talking about at runtime, you can say set threads equals three, or you could say — I'm not sure, Gabor, do you know how to unset when you say set threads equals three? Maybe it's just unset threads, reset threads, and that'll just give you the max threads there. So your extension can either control that, or it can go by the user specification.

>> Yes sir. So when you say —

>> No. So the fun thing here is, DuckDB is really in control. So you're giving that hint.

>> I'm giving it a hint, but it may say, hey, I'm using three of the normal threads for this other table over here. I'm not going to launch you with that many, or those threads don't get time allocated to them on your OS. So you may not actually receive work on them. It's really hard to guarantee every thread is going to get a piece of work.

>> Yes sir.

>> htop. [laughter] I'm going to go with the crowd on that. htop is generally how I look at it. I don't think it's actually in explain, but the profiler may have it. If Maya is here or someone like that, they may be able to tell you. But I don't think it's in an explain right now.

>> Yes. My table function. Uh-huh. What I will get is —

>> I'm not quite sure what you mean. So you mean like statistically?

>> Yeah. So this —

>> Oh yeah, these statistics are only really suitable for the function that we're implementing here, because since we know start and end of the interval, we can tell you what the statistics will be. But say I was writing a table function that fetches stuff from the web. You have no idea. You can't really predict what those values will be. So those statistics that you return there are probably just going to say unknown. But if you can say it's not null, it's still worth telling DuckDB it's not going to be null.

>> Yeah. Because you gave the example, when it's out of range, it does not even run.

>> Yes.

>> So statistics are considered —

>> What I would say is covering, meaning DuckDB trusts them. It doesn't go back and reverify them.

So wrapping up, what we didn't cover today: projection. Projection's kind of like, hey, I have a table function that makes a thousand columns, but I'm only doing a select of two. It makes sense to only have that function produce those two columns, rather than returning those thousand. Projection is that type of thing. Filter pushdown and filter pruning — that's kind of like taking the where clauses and pushing them into the function. That's a pretty cool thing to do, because now if you're say doing an external fetch, or an external service that you're interacting with, you can now say — say we had a table function that's returning the current list of processes, like ps, and you say select star from ps where user equals rusty. It'd be much nicer, rather than yielding all the other processes that aren't owned by me to the function, you can just put that filter in your table function, apply that upstream from DuckDB, and then just give it the data that it needs. Var, that's kind of like taking multiple arguments, one to n number of arguments. And then named arguments are kind of optional arguments that you can have for your function, only supported by table functions, that you can default or let people pass, but they're set with saying option name, walrus operator — so colon equal — and then the value to specify them, rather than positional arguments that we've all demonstrated here today.

Finally, we're getting to the fun part of publishing your extension. So once you're done writing all these things, you have this repo called community-extensions that the DuckDB Labs team has built, and you just create a new file, a new YAML file under extensions/extensionname/description.yml, and you put in a common formatting here of just following the existing format of docs and some basic extension metadata. And then you put in your repo of query-farm/workshop, and then your GitHub reference of what version you want to publish, and you make the PR, and then it's put into community, and that's it. And then you can just do install workshop from community, load workshop, and the whole world can use your extensions.

And I will show you how successful that has been for just a second here. So Query Farm, the company that I run, has published 30 different extensions, and they've taken off. I don't know who's out there. Hello to anyone who's using the Query Farm extensions on the internet. But we just did a 1.4.4 release, so there's some CI out there. But you can tell there's been like 76,000 instances of DuckDB that have loaded one of the Query Farm extensions yesterday. So the audience for extensions is out there, but we don't know who this is, or what they do, or where they're — what company they're from. We don't keep any of that data, but we can just tell you the extension was loaded into the process that many times. So what I've seen is, it grows — as it comes up, it comes down. But it really does have a cool ecosystem that is growing of extensions, and you guys here in the room are all going to write your own, I hope, and join me with giving more functions to these users out there. And the audience is there, and they're hungry for new functionality. So anything I can do to support you, or answer your questions, or give you ideas, I'm here for it.

>> Extensions, are they tied to —

>> They are tied to DuckDB releases, actually. So we have the DuckDB release calendar of LTS, for long-term support, which is our 1.4 branch, and we're going to have 1.5 soonish. So we're going to have — 1.4 extensions will continue to be built, and then 1.5 will also be built, and we'll keep LTS running and the normal release cadence. I don't know what we call non-LTS, but we'll have two branches of extensions moving forward. And as you release, those extensions should be rebuilt for each version of extensions. But this is starting to get into Sam's talk that'll be this afternoon of what the future of extensions will be.

>> Yes. Behavior.

>> Well, this is really — we're getting into policy, and I'm not part of DuckDB Labs, just to be very clear. Like, I'm very grateful to be here, but I am not officially telling you what it is. Talk to Gerard and those guys. But community extensions is kind of like pip, or PyPI. Anyone can publish things there, but there are no guarantees about what they're publishing, or the quality or contents thereof. So it's kind of like no warranties, open source, read the license, and that's kind of where it is. And as for me to say if there's any — I can't really say, but I know that there are tons of extensions getting published every day, and I like to see them, I like to use them, and I'd like to see more extensions to be there. But as for core extensions, I think there's a real difference there, where the DuckDB Labs team has control and authorship over them, and really there's a difference between a core extension and a community extension.

>> Yes. Platform.

>> Yes. So do we have to do anything special to make our extensions also cross-platform?

>> No, actually. Let me show you. If we go to one of my extensions — so we go to GitHub here. I'll show you what happens. So Airport's a good extension. It's what I spoke at at DuckCon about a year ago. And if we go to actions, you'll see these builds here. And it's failing, but that doesn't really matter. I'll fix it later. So when you build — I'm going to go to an extension that actually works, because now I'm embarrassed, and this will be on the internet, and everyone will be sad. You know what I mean? We'll go to one that probably is building fine. Go to Tera. Yay. Green check mark. All right, I'm so much less embarrassed. So this is our actions. And you'll see here that the extension template actually builds all the different platforms. So it has Linux, AMD64, ARM64, and then the different glibcs that we're using, and then it has macOS and Windows, and then WASM at the bottom. So the extension CI tools will actually build all those other platforms for you, and you can exclude them if your extension is not supposed to be built on that platform for no support.

>> Okay. So because earlier you mentioned, say, reading Parquet files — if my extension reads Parquet files, reading files on Windows is very different from —

>> Yes. And I would say, use the DuckDB file system handling that has the correct abstractions across those platforms. So you don't have to worry about what the platform-specific code is, unless you want to go there.

>> Yes. I hope so, rather trying.

>> Okay, this is — how long do you got? You know what I mean? So I think I'm going to give you the shorter answer. I've tried to build a lot of extensions to figure out that same need of, like, where do users of DuckDB need the functionality to be built out, and it's been really hard to figure out, and I've had some hits and some misses. I can tell you, like, some extensions are super popular and some extensions aren't super popular. But for the people that are using an extension, it's the greatest thing since sliced bread. But should we count the number of times an extension is used as usefulness? Is that the right measure? Or is a better measure the broadness of an extension across a space? So I guess I'm going to have to defer to the crowd, but I think a measurement of popularity of number of times it's loaded is a good measurement, but I also think having an extension in an area of a vertical, say statistics and statistical distributions, like I wrote with Stochastic, was — a uncovered area of a SQL that needed to be, the basic functionality coming in would be really good, because it isn't really able to be done inside the normal built-in functions of DuckDB. So my answer is, put a lot of fishing hooks out in the water and see what fish you catch, to be guided by what the marketplace needs. But I defer to the rest of the room of what they think.

>> If I can make a comment.

>> Sure.

>> I like to scratch my own — very useful, facing a problem, build an extension for it.

>> That makes sense to me. At least you will serve yourself and your colleagues. And I think the other thing I would say is, in the last couple months, Claude has really made — Claude and other coding assistants have made extension creation much easier. So, and I think DuckDB Labs, and all the community people, are trying to make extension building much less of a niche industry and be more of a, okay, hey, I need an extension to go do this, and it will generate that, and you'll load it, and if it's popular, we'll publish it out to community, and we'll get going. And if you look at the growth — and I'll show you this page over here, which I think will really illustrate the growth of the extensions. Down here we have extension ecosystem report, and this is the growth of extensions from say 1.0 all the way up to 1.4.2. So we've gone from about 15 all the way to 107 of when I last generated this web page. And you'll see down here of platform trends of like which platforms are actually getting more extensions. And then this is the pace of our extensions being added and removed per release of DuckDB. So you can tell, as we've been in the 1.4 branch, a lot more extensions have been created over time than say in the 1.0 or 1.2 branches. So the growth is accelerating of extensions, and I think all of us will make the community better, and DuckDB will also build out more tools to get this ecosystem more visible. Because I think one thing in DuckDB right now is, the extension ecosystem isn't as visible as it could be, and I think that will change as the ecosystem has grown up, and as more extensions are built, it becomes more important to have this functionality surfaced up for our users. Because I think right now, if you talk to people, they're like, DuckDB is great for Parquet and reading CSVs, but they don't know about these other 150 extensions that exist that add additional functionality for, say, fuzzy string matching has been one of the most popular extensions I've written. And they don't know that that's in existence out there.

>> Yes. Thank you for the workshop. We have seen function-related, scalar and functions. Are there other kinds of function we can develop, this extension, thinking about if we want to implement something that makes automatic data masking, select? Can we do that?

>> Automated data masking. So there's a thing called table in-out functions, which allow you to pipe the result of a query and you return a table as well. So if I were to say a PII masking function, I would say select star from PII mask, open bracket, and then put my subquery in there, and then close that. And what that would do is stream all of the data from the original query into PII mask. It would do whatever magic it needs to do to determine, hey, is this column named name or something like that, and then do the masking, and then just yield out what the results would be, but masked.

>> This — on this, this function, if it's loaded, it will be automatic — automatic call when we do —

>> No, you must actually call it to perform that masking.

>> Okay.

>> As for putting like a proxy function there, that would require you to manipulate the parsing, and yes, you can do fun things, but that's a little bit more complicated.

>> In a view.

>> Sure. Any function can be used in a view, and I think even reading Parquet files and CSV files are just functions. So if you read Parquet or read CSV, your extension could provide the same type of functionality there.

>> Yes. Yeah.

>> Well, in this case, I would say a SharePoint file system is what you need. A SharePoint file system is what you need. I wouldn't change read CSV at all, because that's just CSV parsing. But if you said SharePoint slash or some type of scheme like that, and you provided that as a normal file system, like we have for S3 or Azure, any of those other cloud providers, it would totally work in the same way.

>> Yes. Can you write extensions —

>> Yeah, you can. I've written a couple in Rust, and I've mostly done — there's a — I think it was a couple weeks ago I saw a C# community member, is you can build them there. You can build them in Rust. You could build them in anything that has C-based FFI boundaries. But as for, are they as polished as the C++ and C extension APIs? Probably not. But come to the talks this afternoon. I think we'll hear some great news about what they're doing there.

>> Yes. Yes.

>> It's possible, because even Airport has some different operators like that. This is starting to get in the talks this afternoon, and I don't want to steal any of their thunder, but I think what I can say is that C++ binding extensions into the C++ core has always been something that DuckDB Labs has supported, but not actually given a ton of stability guarantees around. But as we started to build the C API and the C++ API based on top of the C API, the stability guarantees will be there, and also it will make the compilation and linking phases much faster, because it'll go against a standard C API rather than linking against DuckDB core. As they build out more stability guarantees, they'll have more of the API exposed. But will they ever reach the point where every interface is in the extension API and stably there for extension? That we'll have to see this afternoon.

>> Question. Okay. For example, if I extension or something —

>> So I've written the Airport extension, and it has time travel abilities, and that's just in the bind call where those functions are there. But say we wanted to extend the query parameters, rather than saying like at time or at version, we want to say at something else — that would require a parser extension to interface with, and then you do the parsing, and then it would call that bind phase again. Now the exciting thing is, we have the PEG parser coming. I'm not sure if it is in one — again, I'm outside their company, so I don't know what's going to be shipped or not, but the PEG parsing is going to allow us to do extensions of parsing much easier. Because right now it seems like, if a parse error happens, your extension can now be called with the query that couldn't be parsed, can then proceed with that query that wasn't parsible, and then do some recovery there.

Anything else, you guys? I think we're at our end. Thank you for coming. I really appreciate you showing up so early and listening to me for two hours. And I hope it wasn't terribly boring. If you have more questions, reach out to me, and I'll be around this afternoon, and I'm so happy to see all of you, and grow the community of Extension Builders. So thank you very much. [applause]
