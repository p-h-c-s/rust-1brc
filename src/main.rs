use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::thread::{self, Scope, ScopedJoinHandle};

use mmap::Mmap;
use mmap::MmapChunkIterator;

pub mod mmap;

// Defined in challenge spec
const MAX_STATIONS: usize = 10000;
const NUM_CONSUMERS: usize = 32;
const FIXED_POINT_DIVISOR: f64 = 10.0;

struct StationData {
    min_temp: i32,
    max_temp: i32,
    count: i32,
    temp_sum: i32,
}

impl fmt::Display for StationData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:.1}/{:.1}/{:.1}",
            (self.min_temp as f64 / FIXED_POINT_DIVISOR),
            self.get_mean(),
            (self.max_temp as f64 / FIXED_POINT_DIVISOR)
        )
    }
}

/// Efficiently handles station statistics. Avoids using floating-point arithmetic to speed-up processing.
/// The mean is only calculated on demand, so we avoid calculating it as we read the file
impl StationData {
    fn new(temp: i32) -> Self {
        Self {
            min_temp: temp,
            max_temp: temp,
            count: 1,
            temp_sum: temp,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "{:.1}/{:.1}/{:.1}",
            (self.min_temp as f64 / FIXED_POINT_DIVISOR),
            self.get_mean(),
            (self.max_temp as f64 / FIXED_POINT_DIVISOR)
        )
        .into_bytes()
    }

    fn get_mean(&self) -> f64 {
        (self.temp_sum as f64 / self.count as f64) / FIXED_POINT_DIVISOR
    }

    fn update_from(&mut self, temp: i32) {
        self.max_temp = self.max_temp.max(temp);
        self.min_temp = self.min_temp.min(temp);
        self.count += 1;
        self.temp_sum += temp;
    }
    fn update_from_station(&mut self, src: &mut Self) {
        self.max_temp = self.max_temp.max(src.max_temp);
        self.min_temp = self.min_temp.min(src.min_temp);
        self.temp_sum += src.temp_sum;
        self.count += src.count;
    }

    #[inline]
    fn parse_temp(bytes: &[u8]) -> i32 {
        let mut result: i32 = 0;
        let mut negative: bool = false;
        for &b in bytes {
            match b {
                b'0'..=b'9' => {
                    result = result * 10 + (b as i32 - b'0' as i32);
                }
                b'.' => {}
                b'-' => {
                    negative = true;
                }
                _ => panic!("wrong format for temperature"),
            }
        }
        if negative {
            return -result;
        }
        result
    }

    #[inline]
    fn parse_data(line: &[u8]) -> (&[u8], i32) {
        let semicolon_pos = line.iter().position(|&b| b == b';').unwrap();
        let name = &line[..semicolon_pos];
        let temp = &line[semicolon_pos + 1..];
        (name, Self::parse_temp(temp))
    }
}

fn merge_hashmaps<'a>(
    mut dest: HashMap<&'a [u8], StationData>,
    src: HashMap<&'a [u8], StationData>,
) -> HashMap<&'a [u8], StationData> {
    for (k, mut v) in src {
        dest.entry(k)
            .and_modify(|e| e.update_from_station(&mut v))
            .or_insert(v);
    }
    dest
}

/// Parses a chunk of the input as StationData values.
fn process_chunk<'a>(current_chunk_slice: &'a [u8]) -> HashMap<&'a [u8], StationData> {
    let mut station_map: HashMap<&[u8], StationData> = HashMap::with_capacity(MAX_STATIONS);
    let mut start = 0;
    while let Some(end) = current_chunk_slice[start..].iter().position(|&b| b == b'\n') {
        let line = &current_chunk_slice[start..start + end];
        let (name, temp) = StationData::parse_data(line);
        station_map
            .entry(name)
            .and_modify(|e| e.update_from(temp))
            .or_insert(StationData::new(temp));
        start += end + 1; // move to the start of the next line
    }
    // If we don't find a \n, process the remaining data
    if start < current_chunk_slice.len() {
        let line = &current_chunk_slice[start..];
        let (name, temp) = StationData::parse_data(line);
        station_map
            .entry(name)
            .and_modify(|e| e.update_from(temp))
            .or_insert(StationData::new(temp));
    }
    station_map
}

fn process_mmap<'scope, 'env>(
    mmap: Mmap<'env>,
    s: &'scope Scope<'scope, 'env>,
) -> HashMap<&'env [u8], StationData> {
    let mut handlers: Vec<ScopedJoinHandle<HashMap<&[u8], StationData>>> = Vec::new();

    for chunk in MmapChunkIterator::new(mmap, NUM_CONSUMERS) {
        let h = s.spawn(move || process_chunk(chunk));
        handlers.push(h);
    }

    let mut station_map: HashMap<&[u8], StationData> = HashMap::with_capacity(MAX_STATIONS);
    for h in handlers {
        let inner_station = h.join().unwrap();
        station_map = merge_hashmaps(station_map, inner_station);
    }
    station_map
}

fn write_output_to_stdout(station_map: HashMap<&[u8], StationData>) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    let mut buffer = Vec::new();

    buffer.extend_from_slice(b"{");

    let mut sorted_key_value_vec: Vec<_> = station_map.iter().collect();
    sorted_key_value_vec.sort_by_key(|e| e.0);

    for (i, (name, data)) in sorted_key_value_vec.iter().enumerate() {
        if i > 0 {
            buffer.extend_from_slice(b", ");
        }
        buffer.extend_from_slice(name);
        buffer.extend_from_slice(b"=");
        buffer.extend(data.to_bytes());
    }

    buffer.extend_from_slice(b"}");

    stdout.write_all(&buffer)
}

fn main() -> io::Result<()> {
    // won't accept non-utf-8 args
    let args: Vec<String> = env::args().collect();
    let file_name = match args.get(2).clone() {
        Some(fname) => fname,
        None => "measurements.txt",
    };
    let f = File::open(file_name)?;
    let mmap = mmap::Mmap::from_file(f);

    thread::scope(|s| {
        let station_map = process_mmap(mmap, s);
        write_output_to_stdout(station_map).unwrap();
    });

    Ok(())
}
