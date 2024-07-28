use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::str::from_utf8_unchecked;
use std::thread::{self, Scope, ScopedJoinHandle};

use mmap::Mmap;
use mmap::MmapChunkIterator;

pub mod mmap;

// Defined in challenge spec
const MAX_STATIONS: usize = 10000;
const NUM_CONSUMERS: usize = 31;
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

    fn get_mean(&self) -> f64 {
        (self.temp_sum as f64 / self.count as f64) / FIXED_POINT_DIVISOR
    }

    #[inline]
    fn update_from(&mut self, temp: i32) {
        self.max_temp = self.max_temp.max(temp);
        self.min_temp = self.min_temp.min(temp);
        self.count += 1;
        self.temp_sum += temp;
    }
    #[inline]
    fn update_from_station(&mut self, src: &mut Self) {
        self.max_temp = self.max_temp.max(src.max_temp);
        self.min_temp = self.min_temp.min(src.min_temp);
        self.temp_sum += src.temp_sum;
        self.count += src.count;
    }

    #[inline]
    fn parse_temp<'a>(temp: &'a str) -> i32 {
        let mut result: i32 = 0;
        let mut negative: bool = false;
        for ch in temp.chars() {
            match ch {
                '0'..='9' => {
                    result = result * 10 + (ch as i32 - '0' as i32);
                }
                '.' => {}
                '-' => {
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
    fn parse_data<'a>(raw: &'a str) -> (&'a str, i32) {
        let (name, temp) = raw.split_once(";").unwrap();
        (name, Self::parse_temp(temp))
    }
}

fn merge_hashmaps<'a>(
    mut dest: HashMap<&'a str, StationData>,
    src: HashMap<&'a str, StationData>,
) -> HashMap<&'a str, StationData> {
    for (k, mut v) in src {
        dest.entry(k)
            .and_modify(|e| e.update_from_station(&mut v))
            .or_insert(v);
    }
    dest
}

/// Parses a chunk of the input as StationData values. Assumes the input data contains
/// valid utf-8 strings. Also assumes the input data contains whole lines as defined by the challenge
fn process_chunk<'a>(current_chunk_slice: &'a [u8]) -> HashMap<&'a str, StationData> {
    let mut station_map: HashMap<&str, StationData> = HashMap::with_capacity(MAX_STATIONS);
    let str_slice = unsafe { from_utf8_unchecked(current_chunk_slice) };
    str_slice
        .lines()
        .map(|l| StationData::parse_data(l))
        .for_each(|(name, temp)| {
            station_map
                .entry(name)
                .and_modify(|e| e.update_from(temp))
                .or_insert(StationData::new(temp));
        });
    return station_map;
}

fn process_mmap<'scope, 'env>(
    mmap: Mmap<'env>,
    s: &'scope Scope<'scope, 'env>,
) -> HashMap<&'env str, StationData> {
    let mut handlers: Vec<ScopedJoinHandle<HashMap<&str, StationData>>> = Vec::new();

    for chunk in MmapChunkIterator::new(mmap, NUM_CONSUMERS) {
        let h = s.spawn(move || process_chunk(chunk));
        handlers.push(h);
    }

    let mut station_map: HashMap<&str, StationData> = HashMap::with_capacity(MAX_STATIONS);
    for h in handlers {
        let inner_station = h.join().unwrap();
        station_map = merge_hashmaps(station_map, inner_station);
    }
    station_map
}

fn main() -> io::Result<()> {
    // won't accept non-utf-8 args
    let args: Vec<String> = env::args().collect();
    let file_name = match args.get(2).clone() {
        Some(fname) => fname,
        None => "sample.txt",
    };
    let f = File::open(file_name)?;
    let mmap = mmap::Mmap::from_file(f);

    thread::scope(|s| {
        let station_map = process_mmap(mmap, s);

        let mut stdout = io::stdout().lock();
        stdout.write(b"{").unwrap();
        let sorted_key_value_vec = {
            let mut v = Vec::from_iter(station_map);
            v.sort_by_key(|e| e.0);
            v
        };
        let (last, vec_content) = sorted_key_value_vec.split_last().unwrap();
        for (k, v) in vec_content {
            write!(stdout, "{}={}, ", k, v).unwrap();
        }
        write!(stdout, "{}={}}}", last.0, last.1).unwrap();
    });

    Ok(())
}
