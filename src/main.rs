use std::collections::BTreeMap;
use std::{default, env};
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::io;
use std::rc::Rc;
use std::str::{from_utf8, from_utf8_unchecked};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, ScopedJoinHandle};

pub mod mmap;

// station_name limitations: 100 bytes max
// treat temperature as 3
struct StationData {
    min_temp: i32,
    max_temp: i32,
    count: i32,
    temp_sum: i32,
}

impl StationData {
    fn new(temp: i32) -> Self {
        Self {
            min_temp: temp,
            max_temp: temp,
            count: 1,
            temp_sum: temp,
        }
    }

    fn update_from(&mut self, temp: i32) {
        self.max_temp = self.max_temp.max(temp);
        self.min_temp = self.min_temp.min(temp);
        self.count += 1;
        self.temp_sum += temp;
    }

    fn update_from_station(&mut self, src: Self) {
        self.max_temp = self.max_temp.max(src.max_temp);
        self.min_temp = self.min_temp.min(src.min_temp);
        self.temp_sum += src.temp_sum;
        self.count += 1;
    }

    fn parse_temp<'a>(temp: &'a str) -> i32 {
        let mut result: i32 = 0;
        let mut negative: bool = false;
        for (i, ch) in temp.chars().enumerate() {
            match ch {
                '0'..='9' => {
                    result = result * 10 + (ch as i32 - '0' as i32);
                },
                '.' => {}
                '-' => {
                    negative = true;
                }
                _ => panic!("wrong format for str")
            }
        }
        if negative {
            return -result;
        }
        result
    }

    // slow!
    fn parse_data<'a>(raw: &'a str) -> (&'a str, i32) {
        let (name, temp) = raw.split_once(";").unwrap();
        (name, Self::parse_temp(temp))
    }

}

// merges src into dest, consuming both
fn merge_btrees<'a>(mut dest: BTreeMap<&'a str, StationData>, src: BTreeMap<&'a str, StationData>) -> BTreeMap<&'a str, StationData>{
    src.into_iter().for_each(|(src_key, src_val)| {
        match dest.get_mut(&src_key) {
            Some(dest_v) => {
                dest_v.update_from_station(src_val);
            },
            None => {
                dest.insert(src_key, src_val);
            }
        }
    });
    dest
}

fn get_round_robin<'a, T>(v: &'a Vec<T>, mut state: usize) -> (&'a T, usize) {
    let item = v.get(state % v.len()).unwrap();
    state += 1;
    (item, state)
}

// find the nearest newline to the end of the given chunk.
// chunk_num should  start at 0
fn get_nearest_newline<'a>(slice: &'a [u8], chunk_num: usize, chunk_size: usize, last_chunk_offset: usize) -> (&'a [u8], usize) {
    let end_idx = (chunk_num + 1) * chunk_size;
    match slice[end_idx..].iter().position(|x| *x == b'\n') {
        Some(i) => (&slice[(end_idx-chunk_size+last_chunk_offset)..(i+end_idx)], i+1), //+1 cause start of slice is inclusive
        None => (&slice[(end_idx-chunk_size+last_chunk_offset)..(end_idx)], 0)
    }
}

// Defined in challenge spec
const MAX_STATIONS: usize = 10000;
const MAX_STATION_NAME_SIZE: usize = 100;
// 5 bytes for two digit float number with a single fractional digit and `;` character
// idea to divide file: pad each line up to MAX_LINE_SIZE bytes
const MAX_LINE_SIZE: usize = MAX_STATION_NAME_SIZE + 5;
const NUM_CONSUMERS: usize = 16;

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
    let mmap = mmap::Mmap::from_file(f);

    let chunk_size = f_size as usize / NUM_CONSUMERS;

    let station_map = thread::scope(|s|{
        let mut handlers: Vec<ScopedJoinHandle<BTreeMap<&str, StationData>>> = Vec::new();
        // let file_string_slice = unsafe {from_utf8_unchecked(mmap)};
        let mut last_chunk_offset: usize = 0;
        for chunk_num in 0..NUM_CONSUMERS {
            let new_line_data = get_nearest_newline(mmap, chunk_num, chunk_size, last_chunk_offset);
            let current_chunk_slice = new_line_data.0;
            last_chunk_offset = new_line_data.1;

            let h = s.spawn(move || {
                let mut station_map: BTreeMap<&str, StationData> = BTreeMap::new();
                let lines = unsafe{from_utf8_unchecked(current_chunk_slice)};
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
            });
            handlers.push(h);
        }
        let station_map: BTreeMap<&str, StationData> = BTreeMap::new();
        handlers.into_iter().fold(station_map, |s1, s2| {
            let inner_station = s2.join().unwrap();
            merge_btrees(s1, inner_station)
        })
    });

    {
        // write to stdio
        let mut stdout = io::stdout().lock();
        stdout.write(b"{").unwrap();
        for (k, v) in station_map.into_iter() {
            // ("{}={}/{}/{}", k, v.min_temp, v.mean_temp, v.max_temp)
            write!(
                stdout,
                "{}={}/{}/{}, ",
                k, v.min_temp, v.count, v.max_temp
            ).unwrap();
        }
        stdout.write(b"}").unwrap();
    }


    Ok(())
}
