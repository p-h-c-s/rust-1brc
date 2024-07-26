https://github.com/gunnarmorling/1brc

The task is to write a program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this (i.e. sorted alphabetically by station name, and the result values per station in the format `<min>/<mean>/<max>`, rounded to one fractional digit):

example output:
```
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
```

`<string: station name>;<double: measurement>`

Station name: non null UTF-8 string of min length 1 character and max length 100 bytes, containing neither ; nor \n characters. (i.e. this could be 100 one-byte characters, or 50 two-byte characters, etc.)

Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit



<!-- cargo run -r src/main.rs measurements.txt | head -c 5000 -->

To profile: 
cargo install flamegraph
<!-- With 
[profile.release]
debug = true
in cargo.toml
 -->
sudo cargo flamegraph 

https://aquasecurity.github.io/tracee/v0.16/docs/events/builtin/syscalls/madvise/

https://stackoverflow.com/questions/7222164/mmap-an-entire-large-file


java bench on battery:  80.52s user 18.94s system 357% cpu 27.817 total

rust bench on battery: 237.96s user 25.23s system 657% cpu 40.027 total