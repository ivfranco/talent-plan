# Building blocks 2

### Reading: The Design and Implementation of a Log-Structured File System

If the major performance gain of a log-structured file system is less seeking, does that mean
it's less useful on today's SSDs?

### Reading: Error Handling in Rust

> Update (2020/01/03): A recommendation to use failure was removed and replaced with a
> recommendation to use either Box<Error + Send + Sync> or anyhow.

`anyhow` turns Rust error handling into Go error handling with syntax sugars. Convenient to write
but harder to use at call sites (it's very annoying when a specific error kind cannot be
distinguished from others, e.g. what happened in my implementation of project-2).
`anyhow::Error::context` is useful when the error happened deep down the call stack, but the same
information can be more naturally captured by a logger IMHO.

# Project 2

A note on serde_json: the non-streaming deserializer will check if the character after the value
is EOF or whitespace and throw an error otherwise, there's no way to match against a specific
ErrorCode from serde_json (it's private), as a result serde_json::StreamDeserializer which skips
this check is the only way to read a JSON value from the middle of an input stream.

# Building blocks 3

### Exercise: Serialize and deserialize 1000 data structures with serde (BSON)

BSON is the only one which cannot directly serialize a T: Serialize to a Writer. Is that inherent
or is it a design choice?

### Exercise: Write a Redis ping-pong client and server with serialized messages

It's much more verbose than I thought, end up cutting all the possible corners as RESPv2 doesn't
support struct. Encountered a recursive type bound which rustc cannot resolve (similar to [this
stackoverflow
question](https://stackoverflow.com/questions/53405287/whats-going-on-with-this-bizarre-recursive-type-error-in-rust)
but with BufRead).

### Reading: Statistically Rigorous Java Performance Evaluation

In contrast to Java, Rust doesn't have:

- JIT compilation - VM variation / warming up - GC

A few point still applies such as:

> the first VM invocation in a series of measurements may change system state that persists
> past this first VM invocation, such as dynamically loaded libraries persisting in physical
> memory or data persisting in the disk cache. To reach independence, we discard the first VM
> invocation for each benchmark from our measurements and only retain the subsequent measurements

# project 3

`sled` by default caches all writes and only flushes to disk every 1000ms, a few tests spawns the
server on a child process then calls `std::process::Child::kill` on to terminate it. At least on
x86_64-pc-windows-msvc, `std::process::Child::kill` will skip `Drop` implementations, when used
as a KvsEngine the `sled::Db` must be flushed after every modifying operation otherwise a few
tests won't pass.

### Benchmarks

- Write: about the same speed, heavily IO bounded - Read: `sled` is more than 3 magnitude faster,
  possibly because its in-memory cache

# Building block 4

### Reading: Fearless Concurrency with Rust

> In today's Rust, concurrency is entirely a library affair ... And that's very exciting,
> because it means that Rust's concurrency story can endlessly evolve, growing to encompass new
> paradigms and catch new classes of bugs.

Then we had `Future`.

# Project 4

The introduction of lock-free readers made concurrent reads 5 times faster than the initial
implementation that's a giant mutex locking everything.

# Project 5

In the latest release (v0.34.6 right now)
[`sled::Tree::flush_async`](https://docs.rs/sled/0.34.6/sled/struct.Tree.html#method.flush_async)
when called concurrently causes deadlock:

https://github.com/spacejam/sled/issues/1308

version of sled in Cargo.toml is locked to a recent commit that doesn't have this issue.
