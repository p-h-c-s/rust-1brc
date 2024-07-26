use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::str::from_utf8_unchecked;
use std::thread::{self, Scope, ScopedJoinHandle};
use std::env;

pub mod mmap;

// Defined in challenge spec
const MAX_STATIONS: usize = 10000;
// Parallelism
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
/// The mean is only calculated on 
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
    fn update_from_station(&mut self, src: Self) {
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
                _ => panic!("wrong format for temp"),
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

/// Merges src into dest, consuming both and returning dest.
/// Initally the entry api was used, but the current method with get_mut is slightly faster
fn merge_hash<'a>(
    mut dest: HashMap<&'a str, StationData>,
    src: HashMap<&'a str, StationData>,
) -> HashMap<&'a str, StationData> {
    src.into_iter()
        .for_each(|(src_key, src_val)| match dest.get_mut(&src_key) {
            Some(dest_v) => {
                dest_v.update_from_station(src_val);
            }
            None => {
                dest.insert(src_key, src_val);
            }
        });
    dest
}

/// Returns a slice chunk of size: chunk_size + n, where n is the extra space between the end of
/// the current chunk and the next newline. This makes sure each chunk has whole lines.
/// The input `last_chunk_offset` is needed in order to make sure we don't overlap chunks.
fn get_nearest_newline<'a>(
    slice: &'a [u8],
    chunk_num: usize,
    chunk_size: usize,
    last_chunk_offset: usize,
) -> (&'a [u8], usize) {
    let end_idx = (chunk_num + 1) * chunk_size;
    match slice[end_idx..].iter().position(|x| *x == b'\n') {
        Some(i) => (
            &slice[(end_idx - chunk_size + last_chunk_offset)..(i + end_idx)],
            i + 1,
        ), //+1 cause start of slice is inclusive
        None => (
            &slice[(end_idx - chunk_size + last_chunk_offset)..(end_idx)],
            0,
        ),
    }
}

fn process_chunk<'a>(current_chunk_slice: &'a [u8]) -> HashMap<&'a str, StationData> {
    // Mmap::set_sequential_advise(current_chunk_slice);
    let mut station_map: HashMap<&str, StationData> = HashMap::with_capacity(MAX_STATIONS);
    let lines = unsafe { from_utf8_unchecked(current_chunk_slice) };
    for line in lines.lines() {
        let (name, temp) = StationData::parse_data(&line);
        match station_map.get_mut(name) {
            Some(station) => station.update_from(temp),
            None => {
                station_map.insert(name, StationData::new(temp));
            }
        };
    }
    return station_map;
}

fn process_mmap<'scope, 'env>(mmap: &'env [u8], chunk_size: usize, s: &'scope Scope<'scope, 'env>) {
    let mut handlers: Vec<ScopedJoinHandle<HashMap<&str, StationData>>> = Vec::new();
    let mut last_chunk_offset: usize = 0;
    for chunk_num in 0..NUM_CONSUMERS {
        let new_line_data = get_nearest_newline(mmap, chunk_num, chunk_size, last_chunk_offset);
        let current_chunk_slice = new_line_data.0;
        last_chunk_offset = new_line_data.1;

        let h = s.spawn(move || process_chunk(current_chunk_slice));
        handlers.push(h);
    }
    let mut station_map: HashMap<&str, StationData> = HashMap::new();
    for h in handlers {
        let inner_station = h.join().unwrap();
        station_map = merge_hash(station_map, inner_station);
    }
    // write to stdio
    let mut stdout = io::stdout().lock();
    stdout.write(b"{").unwrap();
    let vec = {
        let mut v = Vec::from_iter(station_map);
        v.sort_by_key(|e| e.0);
        v
    };
    for (k, v) in vec[0..vec.len() - 1].iter() {
        write!(stdout, "{}={}, ", k, v).unwrap();
    }
    let last_item = vec.last().unwrap();
    write!(stdout, "{}={}}}", last_item.0, last_item.1).unwrap();
}

fn main() -> io::Result<()> {
    // won't accept non-utf-8 args
    let args: Vec<String> = env::args().collect();
    let file_name = match args.get(2).clone() {
        Some(fname) => fname,
        None => "measurements.txt",
    };

    println!("Reading from {:}", file_name);

    let f = File::open(file_name)?;
    let f_size = f.metadata().unwrap().len();
    let mmap = &mmap::Mmap::from_file(f);

    let chunk_size = f_size as usize / NUM_CONSUMERS;

    thread::scope(|s| process_mmap(mmap, chunk_size, s));

    Ok(())
}
