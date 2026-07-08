# DuckDB Extension Development Workshop — Part 1 — faithful transcript

*Faithful cleanup of the ASR transcript: every word of substance preserved; only filler removed and clear mistranscriptions corrected. Not editorialized. Verbatim source: `raw/2026-01-30_duckdb-extension-development-workshop-part-1.raw.txt`.*

**Published:** 2026-01-30.  **Source:** Workshop.

---

Good morning, everybody. It's — I'm kind of an informal speaker, especially in these types of rooms. So let's just get to know each other a little bit. How many of you guys work at the DuckDB Labs? Okay. So those are the people that ask the real questions when I get it wrong, and my slides are not perfect, but otherwise we're going to have some fun today. And if that's what we do, that's totally cool with me. And if you have questions, feel free to raise your hand, but I think the slides are pretty comprehensive.

So we have a break coming up in about an hour, and we'll get started. So this is kind of our agenda this morning, just to let you know what we're going to learn. I'm not going to read a lot of the slides of this, but if you guys are familiar, raise your hands and we could skip some topics if you wish; or if you're like, "Hey, I don't know what we're talking about here," also trying to say, "Slow down, we need to recover it," and go like that. So I want to have a two-way on this talk rather than just being in a dark room and me talking at you guys for two hours and you getting bored of my voice and LinkedIn posts everywhere and thinking about the coffee.

But part one's going to be pretty light on code. Part two is going to be pretty deep on code. I would advise you — I'm going to bring you into the mental model that I've built over two years of writing extensions, into how I think about DuckDB and the internals and the classes there. And I may have it wrong, but it's worked out so far. And if we need further clarification, we can ask some of the DuckDB Labs guys.

So, about me. I've written 30 extensions. I started a little company called Query Farm, and it's just an extension company. So if you need extensions to be built and don't want to do all of this work yourself, and after this talk you're like, "Man, I don't want to hear about it again," send me an email. I'm happy to help you. My biggest, most public thing that you can see in DuckDB is this ETA counter at the end of when you run your queries in the CLI. I brought in the countdown clock and the prediction algorithm to predict when your queries are going to complete. And I also inspired a little bit of the new prompt in 1.5. So beyond that, I've done quant finance for about the last decade, and I've done a lot of Arrow work, and those types of groups are kind of my friends. So Arrow, Python, DuckDB — the triangle of analytics — that's me.

So let's get into it. Lifecycle of a SQL query inside the engine are these steps. Parsing — transforming it from your SQL to the abstract syntax tree — is kind of where it all starts. We're not going to cover that today in the talk; I'm going to leave parser extensions out. But then, once we get a parse into an AST, we have to bind that AST into the actual types and functions that we're actually going to call inside the engine. So that's that bind phase, which we will talk about quite a bit. There's an optimize phase, where DuckDB rewrites the plan and operations into an easier-to-execute plan, an easier-to-execute organization of the plan. We're also going to skip those today. We're not going to build a custom optimizer, but that could be a future talk. Then we have the init global, which will be called once for the query when we actually start execution, because queries can be rebound multiple times, especially if you're using like the ANY type in SQL — it could trigger the bind phase to run again. Then we have init local, which is a nit that runs per thread, per executor thread. And then finally the execution phase. And we're probably going to talk a lot about bind, and init global, init local, and execute is what we're going to cover.

Everybody okay so far? I know it's early. It's 3:00 a.m. for me. It's 9:00 a.m. for you all. If it's not working, just let me know.

I put this up before, but I have an entire repo that we're going to step through. We're going to build an extension together. We're going to do a scalar function, a table function. If you haven't cloned it, please start the clone and bang on the Wi-Fi, because a recursive clone could be a little bit of bandwidth in the morning. I'll leave this here while you guys do that. Normally I had like a song to play, but let me just — tell me when everybody's like, "I got the URL and we're ready to go."

>> I have the Wi-Fi.

>> I do have the Wi-Fi. There you go, guys. And then this is the repo right here. And then once we get a consensus that everybody has it cloned, we'll go on from there. But in the meantime — trying to think — is this the coldest it's been in Amsterdam, or has this been normal weather? Because when I came here last year, it was a little bit warmer.

>> Yesterday it was quite chilly.

>> Yeah. All right, I see some people. Good. We ready to go? Anybody's like, "Wait, wait, wait." All right, we're going.

So once you clone, you'll see files like this. The way I start DuckDB extensions, I always use the extension template that Sam and Carlo wrote, and it builds in an extension. And these files won't be named exactly the same way. They'll be named quack extension, or quack extension.hpp for the header files. And I did a simple sed rename to just make it workshop rather than quack. There's a couple fun things you have to do there with case sensitivity, but we'll get into that later. Then I have this file here called a test. This is a SQL unit test; we'll get into that. This is build configuration. These you really don't have to touch, but CMakeLists is, because all DuckDB extensions use CMake. And these things kind of tie all together with the make file. And vcpkg.json is for C++ dependencies. And then finally, we have some documentation of a license file and the normal README.

You guys all good? Okay. This is going to start to be — is the source code too small? I can try to zoom it in, but this is the basic start. If you do a `git checkout step-1` on that repo, that'll get us to our first evolutionary step of how we're going on the extension building. And we're going to open — this will be `workshop_extension.cpp`.

And while you open it, if you want to follow along, you can — if you have Ninja installed, you can use that as your build tool. That's a tool that really parallelizes the calls to CMake and C and C++, or your Clang C++ compiler, just with `make debug`. And that can run in the background while we're talking through the source code, because it'll take you about three to four minutes on my laptop to run the DuckDB build if you're not using ccache, which is like a compiler caching tool. And then we'll get started with build test here.

But walking through this code — hello, and welcome.

>> Oh, it's all good.

We have this little part here with the `extern "C"`, which is what's called our hook into the extension. So when DuckDB loads an extension, it calls `dlopen`. A dynamically loaded SO file, or shared library, gets loaded into the same process, and it needs to find a way to — how do we call the initial function to bootstrap that extension? And to do that, C++ will use name mangling, so it's hard to predict what that symbol will be called. So that's why we're using `extern "C"`, and we're going to say `duckdb_extension_entry`, then workshop's our extension name, and our loader. And that's just going to say, "Call load internal," inside the DuckDB namespace, which is — most of the extension code is covered in the DuckDB namespace, where it's like `namespace` at the top, close here. That's the standard that we see. And then inside of there, here's our workshop extension load function, but this calls again a static function called load internal. And this is the basic scaffolding of, when the shared library gets loaded into the process, how do we get started to interact with the rest of the DuckDB engine? And then finally, load internal — and this is where we're actually going to start to put our logic, inside the load internal call, to add our functions, add our functionality, and build our extension out inside of there.

We good to go? Okay. What we're going to do today is we're going to build a function. The first function we're going to build is called Easter, to calculate the day of Easter for a particular year. And Easter varies around. So it's not a simple, "Hey, it's the last Sunday in April, or March, or wherever." It's dealing with equinoxes and lunar phases. And there's a nice Claude-provided anonymous algorithm to calculate the day of Easter using the Gregorian calendar. So that's what we'll be coding, but we're not really going to get into that part of the presentation. We're just going to take that code and paste it in as a piece of logic, but we're going to put all of the scaffolding around it so DuckDB can call it.

Now, if you haven't used DuckDB much, or you're kind of new, it operates in a vectorized manner. So rather than just calling your C or C++ function a thousand times, DuckDB would rather just say, "Hey, I'm going to call you with a thousand sets of parameters to your function, and you go calculate and then return me a thousand results," rather than one by one by one by one. It wants to do like a thousand, 2,000 results. This slide is showing you, like, we're going to do vectorized execution rather than just a scalar execution. And scalar functions mean we just get a single output to multiple inputs, and that's where the scalar part comes. It's not scalar execution, it's actually a scalar result.

So here's where the talk becomes: how did I learn all of this? Bringing the mental model of DuckDB into your head can be really challenging. And I think it's best to — we're going to go through this of starting here at logical types, and values, and vectors. And I'm going to try and give you an understanding of what I've built up of all these C++ classes, so I can think about the right way to understand the internals so I can actually integrate into them well. And this is a mental map of the types as I learned, starting at the top, moving down in functionality. So if you just open up DuckDB and start reading the source code, it's really hard to figure out, "How do I glue it all together? Which way is up, which way is down, where do I start?" And it's really best to probably start here in logical type. And then we'll go into value and vectors and build this direction. We won't talk much about string storage, but we can — vector types, validity mask, we'll get to all of that. And then scalar functions, table functions, generic executors. This will be what we're covering in the talk today.

So let me refresh this. We should have — there's our picture of a logical type. A logical type is the way that we express all of the basic types inside DuckDB's execution model. All of the other types — the physical storage of them is at a lower level. But these logical types can be composed for nested types as well, like our structs, maps, arrays, and lists, and unions. But — how many of you have written C++ before? Okay, so you guys all know about the primitive types, and you all understand like int64s and int32s. These all map into this column here. These are all your standard C++ types. Your date, timestamp — these are interpretations on top of those basic C++ types. Your varchars and your blobs — pointers, and a little bit different of string, and turning inside DuckDB. Hugeints — C++ doesn't quite support them yet in a nice way, so they're handled a little bit differently. And then bits and geometry. This is like a variable-length bit string. And geometry is kind of its own little thing, and I'm not really going to touch on it too much. But these are the basic types, and you build up from there. And as you're building extensions, you're going to use a lot of logical types to say, "These are the types of my arguments. These are the types of my parameters that I accept or produce." So it's really important to understand what types to use.

So in this workshop today, we're mostly going to stick in with dates and integers. And we might get into some strings a little bit later. Any questions on logical types? Don't be shy if you do. There's no bad questions.

>> [Q on why they're "logical."]

Well, they could be logical because you can have composed types, whereas a struct can be a dynamic type with the members actually being specified — because a struct, you have to know the keys are fixed, but the values change. So you can have dynamic types in that way, whereas the physical representation of that in C++, the struct will have individual vectors for each member. So there's not a direct mapping always between a logical type and a single vector C++ primary type. You're welcome. Thanks for the question.

Now, values are simply just a logical type associated with a value. So like if we had -30 and it was a big int, that's totally possible. So just think of value as like a container between a logical type and the actual type itself — like a scalar, single value container. A vector is a collection of multiple values, but they have a logical type, but rather than having the type stored 400, a thousand times, or 2,000 times, it'll just have a single type. Then it has a validity mask, which is how we know in this vector of say 2,000 ints which ones are null and which ones are not null. And we have buffer data, which is, in the case of say we have an integer vector, and it's a flat buffer — it's going to be 2,048 int64s right after each other in that same old contiguous way.

And I was thinking about this a lot at home, and I was like, well, do people see arrays as left to right, or do they see them as top to down, bottom to top? And I think that depends on how you were raised and how you were educated, and I don't know, but I am a left-to-right array person.

>> I guess, but is memory — does memory go left to right, or does memory go down? Like, do you grow from the top or the bottom?

These are hard questions to really solve. And then with vectors you have auxiliary data, and that means you can attach other pieces of memory that that vector references. And this is how strings are really stored inside DuckDB, rather than being contiguous inside a vector. It stores pointers to those strings if they're longer than 12 characters, inside of that vector structure itself. And that's what we're talking about here in `string_t`, where this is how strings are stored inside DuckDB, where it has a length of how long is the string, because it's all UTF-8 and it can be binary. It has a prefix of 12 characters, and then it has a pointer to the actual data where that string is stored. So that could be at any location.

>> Like characters or by—

Well, in this case, this would be bytes, because characters can be code points that take lots of bytes.

>> [Q: linked list?]

No, it's not a linked list. So if the string is longer than 12 characters, it's going to be stored with a pointer to where the actual string is. But if it's less than 12 characters, it'll actually be stored inline with the `string_t`.

>> Yeah, like a CPU cache line purposes.

And a lot of times — there's been good studies that say a lot of strings are pretty short. And a lot of times when you're doing comparisons, if you can do the comparison within 12 characters, it'll be much faster than doing a pointer follow to arbitrary memory locations.

>> That seems so un-American. [laughter]

You know, yes. No, this is a struct inside C++. So there's a length — I think it's like a `size_t`. And then a static `char[24]`, and then the pointer is your standard pointer. All right.

>> Yes.

>> [Q: what's the vector type?]

The vector type of what, of this?

>> Yeah.

There's different vector types. So there's flat types, there's dictionary vectors, there's sequence vectors, there's constant vectors. We're going to get into that in the next slide.

>> Oh, okay. Good.

Okay. And then here we go into data chunks, our collections of vectors that we just covered. And these are all the different types of vector layouts that I wanted to put on a slide that I could actually draw. DuckDB has this cool way that it will optimize vectors. So if you just have 2,000 integers that are all the same value, it really doesn't want them to waste your memory on having 2,000 integers all the same. So it's better to say that's a constant vector of the same value, so you get nicer cache performance. But the typical way that you'd like to work with vectors, in the case of having them from left to right, is an array. And that's called the flat vector, meaning the data is flatly contiguous, one after another. Each value is one after another. And that's what's illustrated in this value here, for the flat vector layout, with a validity mask of saying these elements are null, these elements are not null. And the validity mask is stored as like a binary-packed bit mask. So rather than just being like these are not characters 0 1 0 1 0 1 0, this is just a single bit packed that way.

And then for a dictionary vector, that's combining a child vector, which could be a flat vector of the values, and then just the index in that dictionary of where that value is referenced. So rather than having — if you have a really low canonical, or low number of unique values, vector of long strings, you'd probably want to have a dictionary vector so you don't have to duplicate all that memory over and over.

Now, this is a lot to lay on you guys for mentally modeling how this works. Is everybody okay with vector layouts? Sort of, kind of.

>> So all these different vector types, from the outside they have the same interface in programming requirement?

Well, we're going to do this thing called unification. And there's this thing called unified vector layout, some unified vector format, or something like this — this API call. I won't show you, but there's a function you can do that'll change all of these different layouts into a flat vector, or a uniform vector representation, that allows you to keep them in this layout but access them as if they were like a flat vector. So you really don't have to worry about — and you can also produce these different types of vectors in your extensions to be really efficient for execution later on.

There was a question in the back.

>> Yes.

>> Oh, no problem. So if you have a vector that has a large number of repeating values, and those values are largeish, like non-integers, say strings, you don't want to have the 2,000 copies of the same strings over and over and over again. So you can take the unique values that you have in your vector, and then just store them as a child vector in a dictionary, and then just point to their index — like zero means apple, two means car, zero means apple again, one means banana. You got it.

>> So — what's that?

It's an encoding. Yes, yes.

>> Are they stored in auxiliary data we saw before, or is it—

In the dictionary vector, the strings will be stored in the auxiliary data, but this child vector is actually part of the dictionary vector. So we'll get into it, but to be honest, guys, in two years, I haven't written any code that's actually had to generate a dictionary vector. I've consumed them, but I've never actually had to write them. So it's cool to know it's there, but I didn't bring an example today.

So we're going to get back into building Easter. And this is our name load internal builder extension here, where we had it before. And this is a really fun thing. I kind of like how it morphs the slides. So we can do that again, because it's really fun.

So we're going to call — there's an extension loader that's passed into us, and this is a nice object reference that allows us to do interactions with the main DuckDB catalog. So we're going to say extension loader, register function, and then we're going to walk through it line by line. So Easter here is the name of our function that we're registering, just a normal string. The first argument are the arguments we're going to take, and we're going to take a — this is a C++ vector, which is why I have those curly brackets, an inline definition of a vector, and these are the input arguments to the function. So the first parameter to our function is just going to be a big int, or in a case, int64, which is the year. And then the next parameter to register a scalar function is the return type. So we're going to return a date. And then finally, we just give it the name of our function to call that will calculate our Easter date. Any questions on this so far?

>> Yeah, this is the workshop extension?

Yes. And if you go to step one, I believe this code will be there. Or if you check out the branch step two, the code will be there too. Step hyphen one, or step hyphen two.

>> Okay, let's go to step two, please.

We will check. Let's try step two. Number two. It might be formatted slightly differently, but I also can pull it up in my VS Code. Yes, the starting link.

>> This one.

You're welcome. Okay. So I just checked out step two, and this is what I have. It's a tag. Step two may be a tag, and then it's formatted slightly different on line 38, but I can make it happen for you guys. I think when I made the slides I'd added additional newlines, but in the regular code it's just one line. So we walk through register function, and then — I'll go back to the slide here for just a second. There's this function up here, which is our actual Easter calculation function, and all scalar functions have the same signature on the top, where it takes a data chunk for the arguments coming in, expression state — don't really need that for now — and then our vector, which is our result, where we actually have to write the result. Everybody okay with this so far? Yes.

>> [Q on the scalar/vector result.]

Well, scalar just means it writes a single output value. So it's — and since DuckDB is vector execution, if we have vectors of inputs, we have to have a vector of outputs. So that's why we have a — yeah, in the vector, it's just writing one value in the vector of result, but the result is still a vector.

So we're going to take the first argument, which is our year. So it's like `args.data[0]`, normal state of access there. And then we're going to call this thing called a unary executor. It's a template-y function, which we'll go into, where it takes the input, which is our int64_t, which is our input type, and then our date_t is our return type from the template function. And we just highlighted that. And here's our actual implementation of this C++ lambda, where we have our year vector, which we already got as an arg, the first argument; the result, where we're storing the result; args size is the number of elements in the vector of our input that we were called with to execute our scalar function; and then our square bracket here, ampersand, means our capture list of what we want to capture in the C++ lambda. So we don't need to capture anything from outside of the scope, because everything we need is already being passed to us. And our year is our argument parameter. And this is run once, this is run one, two, three, four, five, six, seven — it's run 2,048 times, the size of the standard vector, because it's the lambda. And I'm going to omit the code of actually calculating the date of Easter, but it's in your extension. And finally, it's going to return the date from those values that I calculate. That was kind of a big leap. Everybody, you got through that explanation. Okay.

So if you update to step two and you build it, you should be able to run `SELECT easter(3000)`. And does it work for you all? I see some people saying yes, because you all are overachievers and you've already compiled it and you've done — yes.

>> Yes, this one.

Okay, I have another slide about executors coming up. Let's go through that slide, and then we'll come back to if you don't get it. All right. While you guys are compiling, and I see some people looking around, so I'll take a minute. But these are our executors, and the DuckDB Labs team has been really, really nice to us, because a lot of times when you run a scalar function, you need to check validity masks. You need to unpack what type of vector did I get passed in. There's a lot of details that just happen that you don't have to worry about if you use these executor template functions, where you can just be like, "Hey, I want to just write my C++ code and isolate all that DuckDB logic and vector handling outside of my implementation." So they have unary, binary, ternary, quaternary — you know, just various numbers of arguments is how they're named. Unary meaning one, binary meaning there's two arguments passed into the function, ternary three. And then there's generic, where you can have n inputs to one output. Vector executors are only useful for scalar functions. They make no sense in a table function context, but they cover a lot of really good functionality that you may mess up. And they also do nice optimizations, where if say the first argument is a constant, it can do some nice handling of not passing that same value in so many times to your function. So there's other optimizations that I'll gloss over, but please just use executors until you can't use executors anymore due to your use case.

>> Sure. [Q about how to build.]

You need to build it using the `make debug`, and then you do `build/debug/duckdb`. So those will be the build targets. If you ran a release build, it would be `build/release/duckdb`, and those builds have your extensions statically linked into it. It's not being dynamically loaded. It's already built in. So there's no need to call load or anything else like that for your use case.

All right. And we're almost done with phase one. So the nice thing about DuckDB functions is that it also allows you to provide documentation. So if you're like me and Gabor, we're always publishing web pages documenting our extensions, and you want to have a way to tie the documentation of the function together with the implementation. So if you ever have used this, or haven't used this, there's a `duckdb_functions()` table function, and you'll see probably about 1,500 built-in functions in that table to return it. But with our new extension called Easter, I'm also giving you the ability to have the description, the parameter types, the return type. So the parameter here is year, the return type is date, some categorization, and some examples. And you can add that to your extension. So your users of your extension will also be able to figure out, "Hey, how do I call what you guys just gave me?" So to do that, we're going to take — this is our initial register function, and we're going to change it into this, where our main function went up here, Easter, the name, main Easter function, but now we're going to create it with additional scalar info, where we're setting a description, examples, categories, parameter names, and then we register the function wrapped in all the extension function info. So if you — I didn't also take a survey of who else has published extensions here in this room; there's probably some of you — please do this, to give nice documentation. And I'm sure Gabor will build tools eventually to extract this from all the extensions that are published in the community collection, to build nice documentation pages.

>> Yes. Uh-huh.

>> Hey. Uh-huh. Uh-huh. Uh-huh. [Q: is it unary?]

No, it's unary, because we have only one argument to the function. We just have the year coming into the function. Let's go back to that slide. We have one, which is the year is the input, and we have the date as the output. So we have unary — the executor is named after the number of inputs. So we have one input means unary, two inputs binary.

>> Sure. Uh-huh.

So on the unary call, I'll clarify here. Unary, execute, int64_t is our input. So that's the year, like 2026, 2019. And then date_t is our return type out of the executor.

>> [Q about running regular DuckDB.]

No, you should — when you build it with your `make debug` and you run it, you don't run regular DuckDB, like the kind you download from Homebrew. You just run it in the — you're in your extension directory, and you do `./build/debug/duckdb`, and that'll be a custom DuckDB build with your extension already loaded statically. Okay, I can help you during the break.

>> [Q about one row at a time.]

Uh, no. So DuckDB does this thing called morsel-based execution. So it will call you with the standard vector size number of rows based on your inputs. So if you just call `SELECT easter(3000)`, like we had in this other slide, it's just going to get called with a single value. But if I have it with 20,000 rows, it's going to call you 2,048 chunks at a time. Does that make sense?

>> [Q: will it call Easter the main function?]

It will call Easter, the main function, because — yeah. No vectors, right? It chunks — DuckDB executes rows in these things called standard vector size vectors, which are 2,048 elements right now.

>> [Q about 2,048 elements.]

And it runs 2,048 elements, and then it streams the results back out through that execution size.

>> Correct. Correct. But inside the executor, it will actually execute that lambda 2,048 times.

>> [Q about controlling it.]

There's DuckDB guys in the room. I'm sure there's a way to control it. But I think, in my understanding as an extension builder, there's no way to really say, "Call me one row at a time," on a scalar function. But you can tell me if I'm wrong.

>> I don't think you do as an extension builder.

Oh — good question. We'll go back here. You do `SELECT star FROM duckdb_functions() WHERE function_name = 'easter'`.

>> [Q about registering a built-in name.]

There's probably going to be an exception raised when you try to register a function with the same name. But I don't think Easter is built in [laughter] — yet. Yet as of when I wrote this presentation, but I know who to talk to about that.

>> Yes.

And we covered this. We have the documentation, and then it's time to update to step three, if you guys are ready for that. And that'll have the function documentation built in for your extension. And then we're going to get into testing. And then we're almost ready for a break. So I know you guys have gone through a lot, and I know you're like, "Oh man, coffee." We'll get there. And I know this is how testing always is. It's like the last part of your development process. But hey, I'm not any different from the rest of you all.

So there's this great file called workshop. It's under `test/sql/workshop`. And it uses this thing called SQLUnit. I haven't been able to decide if the DuckDB team invented this, or it came from TCL. I'm not sure how this thing — does anybody know the lore of how these tests were written?

>> Okay. Do you know where it came from?

>> Okay.

Okay. So we're going to use the SQL unit test as well. And this — I'm not going to explain the whole syntax of it, because I haven't found the documentation page, but I will find it on SQLite and point you guys to it. It's where it's like it's either a statement or a query. And this was saying `statement error`, meaning this statement should raise an error. And then you put the statement, and then you say, like, this is the error I expect to be raised from the engine. And that's going to be, "Hey, if you call Easter without loading my extension, I expect it to say this function doesn't exist." Then the next line's going to say `require workshop`. And workshop is the name of the extension we've been building, which has the Easter function. And then we'll say, after that, if we do `query I`, means I expect one column being an integer type value. `SELECT easter(2026)`, I expect it to be this value. And that's how you write your tests.

And I see a lot of confused faces, and I was kind of the same way. And eventually I wrote enough of these things to feel comfortable with how this is. And they're kind of space-separated files, which I think also trips you up. So when you're in VS Code, you have to go down the bottom right corner, change tabs to spaces, otherwise it's going to be complaining like, "I expected one column, but I got four," or something like that. But it's space-separated values for the tests. So if you were to have a `query I`, `II` would be date, and then whatever the value is. And we'll see that next in our table functions after the break. Any questions on testing?

So then if you do `make test_debug`, means run the test with the debug build, it will run these tests and give you the right assurances about the code is correct. And then here's an example of `query II`, meaning two columns. Here's the year, and here's the Easter day of that particular year. And I just did a little like from range. Range is pretty popular; we'll get into that. So select year, Easter year, and here's our result. Now it's break.
