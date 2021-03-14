# Building blocks 2

### Reading: The Design and Implementation of a Log-Structured File System

If the major performance gain of a log-structured file system is less seeking,
does that mean it's less useful on today's SSDs?

### Reading: Error Handling in Rust

> Update (2020/01/03): A recommendation to use failure was removed and replaced
> with a recommendation to use either Box<Error + Send + Sync> or anyhow.

`anyhow` turns Rust error handling into Go error handling with syntax sugars.
Convenient to write but harder to use at call sites (it's very annoying when a
specific error kind cannot be distinguished from others, e.g. what happened in
my implementation of project-2).
`anyhow::Error::context` is useful when the error happened deep down the call stack, but
the same information can be more naturally captured by a logger IMHO.

### Exercise: Serialize and deserialize 1000 data structures with serde (BSON)

BSON is the only one which cannot directly serialize a T: Serialize to a Writer.
Is that inherent or is it a design choice?
