use std::collections::BTreeMap;
use std::{default, env};
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::io;
use std::rc::Rc;
use std::str::from_utf8;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, ScopedJoinHandle};

pub mod mmap;

// station_name limitations: 100 bytes max
struct StationData {
    min_temp: f64,
    max_temp: f64,
    mean_temp: f64,
    times_seen: f64,
}

impl StationData {
    fn new(temp: f64) -> Self {
        Self {
            min_temp: temp,
            max_temp: temp,
            mean_temp: temp,
            times_seen: temp,
        }
    }

    #[inline]
    fn running_avg(&self, temp: f64) -> f64 {
        (self.mean_temp * (self.times_seen - 1.0) + temp) / self.times_seen
    }

    fn update_from(&mut self, temp: f64) {
        self.max_temp = self.max_temp.max(temp);
        self.min_temp = self.min_temp.min(temp);
        self.mean_temp = self.running_avg(temp);
        self.times_seen += 1.0;
    }

    fn update_from_station(&mut self, src: Self) {
        self.max_temp = self.max_temp.max(src.max_temp);
        self.min_temp = self.min_temp.min(src.min_temp);
        self.mean_temp = self.running_avg(src.mean_temp);
        self.times_seen += 1.0;
    }
    // slow!
    fn parse_data<'a>(raw: &str) -> (String, f64) {
        let (name, temp) = raw.split_once(";").unwrap();
        (name.to_owned(), temp.parse::<f64>().unwrap())
    }

    // fn parse_line_buff<'a>(line_buff: &'a str) -> impl Iterator<Item = &'a str> {
    //     line_buff.lin
    // }

}

// merges src into dest, consuming both
fn merge_btrees(mut dest: BTreeMap<String, StationData>, src: BTreeMap<String, StationData>) -> BTreeMap<String, StationData>{
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
fn get_nearest_newline<'a>(slice: &'a str, chunk_num: usize, chunk_size: usize) -> Result<&'a str, ()> {
    let end_idx = (chunk_num + 1) * chunk_size;
    match slice[end_idx..].find('\n') {
        Some(i) => Ok(&slice[(end_idx-chunk_size)..i+1]),
        None => Err(())
    }
}

// Defined in challenge spec
const MAX_STATIONS: usize = 10000;
const MAX_STATION_NAME_SIZE: usize = 100;
// 5 bytes for two digit float number with a single fractional digit and `;` character
// idea to divide file: pad each line up to MAX_LINE_SIZE bytes
const MAX_LINE_SIZE: usize = MAX_STATION_NAME_SIZE + 5;
const NUM_CONSUMERS: usize = 2;

fn main() -> io::Result<()> {
    // won't accept non-utf-8 args
    let args: Vec<String> = env::args().collect();
    let file_name = match args.get(2).clone() {
        Some(fname) => fname,
        None => "head.txt",
    };
    // let station_map: [StationData; MAX_STATIONS] = [StationData; MAX_STATIONS];

    println!("Reading from {:}", file_name);

    let f = File::open(file_name)?;
    let f_size = f.metadata().unwrap().len();
    let mmap = mmap::Mmap::from_file(f);

    let chunk_size = f_size as usize / NUM_CONSUMERS;

    // works, but is memory intensive
    // Memory limited implementation, but very fast IO

    let station_map = thread::scope(|s|{
        let mut handlers = Vec::new();
        let file_string_slice = from_utf8(mmap).unwrap();
        for chunk_num in 0..NUM_CONSUMERS {
            let curren_chunk_slice = get_nearest_newline(file_string_slice, chunk_num, chunk_size);

            let h = s.spawn(move || {
                let mut station_map: BTreeMap<String, StationData> = BTreeMap::new();
                loop {
                    if let Ok(chunk_slice) = curren_chunk_slice {
                        for line in chunk_slice.lines() {
                            // let fmt_line = &line[0..line.len()-1]; // remove newline
                            let (name, temp) = StationData::parse_data(&line);
                            match station_map.get_mut(&name) {
                                Some(station) => station.update_from(temp),
                                None => {
                                    station_map.insert(name, StationData::new(temp));
                                }
                            };
                        }
                        return station_map;
                    } else {
                        return station_map;
                    }
                }
            });
            handlers.push(h);
        }
        let station_map: BTreeMap<String, StationData> = BTreeMap::new();
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
                k, v.min_temp.round(), v.mean_temp.round(), v.max_temp.round()
            ).unwrap();
        }
        stdout.write(b"}").unwrap();
    }


    Ok(())
}
