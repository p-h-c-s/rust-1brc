# 1 billion row challenge in Rust

This repository contains a solution to the [1 billion row challenge](https://github.com/gunnarmorling/1brc), written in rust.

The solution uses the concept of divide-and-conquer, by dividing the input file into chunks and processing them in parallel.
The IO is done by creating a `mmap` from the challenge file. This allows the solution to achieve read speeds of up to 800Mb/s in my Mac air M1 16GB 1TB of SSD storage.
This implementation also uses little memory. We completely avoid 'user' heap-allocations (however the standard-library might heap-allocate internally). As the number of unique station names is bounded by 10000, the data structures used to aggregate the data are very lightweight. In my tests the program stabilizes at about 8.5Mb.

## Running

To time execution, we can use the time command (we build before run to avoid timing the compiler):
```
cargo build && time cargo run -r src/main.rs measurements.txt
```

The program receives the filename as input. Details on generating the file at:
https://github.com/gunnarmorling/1brc

Time outputs the results like this:
`80.97s user 13.88s system 545% cpu 17.392 total`

With `total` being the actual elapsed time.

## Profiling

To optimize the code while i iterated on it, i used flamegraph from cargo itself:

With root:
`sudo CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph -r -- measurements.txt`

## Iterating

In order to quickly test different implementations i used a sample file containing a tenth of the original lines. This file can be created from the original with the `get_sample.sh` script

## Notes

I wanted to avoid using any external crates. The only exception was `libc` which provides cross-platform C bindings so we could call `mmap`.