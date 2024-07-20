use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::io;
use std::rc::Rc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, ScopedJoinHandle};

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

    // // https://www.reddit.com/r/rust/comments/v8wxky/string_vs_str_as_function_parameters/
    // fn from_string(raw: String) -> (String, Self) {
    //     // unwrap is usually unsafe, but we know the pattern of the input is stable
    //     let (name, temp) = raw.split_once(";").unwrap();
    //     let temp_f = temp.parse::<f64>().unwrap();
    //     (name.to_owned(), Self {
    //         min_temp: temp_f,
    //         max_temp: temp_f,
    //         mean_temp: temp_f,
    //     })
    // }

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

    // fn parse_custom_float(s: &str) -> f64 {
    //     let mut chars = s.chars();
    //     let integer_part: String = chars.by_ref().take_while(|&c| c != '.').collect();
    //     let fractional_part: String = chars.collect();
    
    //     let integer_value: f64 = integer_part.parse().unwrap_or(0.0);
    //     let fractional_value: f64 = fractional_part.parse().unwrap_or(0.0);
    
    //     integer_value + fractional_value / 10.0
    // }

    // slow!
    fn parse_data<'a>(raw: &str) -> (String, f64) {
        let (name, temp) = raw.split_once(";").unwrap();
        (name.to_owned(), temp.parse::<f64>().unwrap())
    }

    fn parse_line_buff<'a>(line_buff: &'a String) -> impl Iterator<Item = &'a str> {
        line_buff.as_str().split_terminator("\n")
    }

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

// struct ChannelPool<T> {
//     pool: Vec<(mpsc::Sender<T>, mpsc::Receiver<T>)>,
//     idx: usize
// }

// impl<T> ChannelPool<T> {

//     fn new(max_concurrency: usize) -> Self {
//         let mut val = Self {
//             pool: Vec::with_capacity(max_concurrency),
//             idx: 0,
//         };
//         for _ in 1..max_concurrency {
//             let channel: (Sender<T>, Receiver<T>) = mpsc::channel();
//             val.pool.push(channel);
//         }
//         val
//     }

//     fn get(&mut self) -> &(mpsc::Sender<T>, mpsc::Receiver<T>) {
//         let item = self.pool.get(self.idx % self.pool.len()).unwrap_or_else(|| {
//             self.idx = 0;
//             self.pool.get(0).unwrap()
//         });
//         self.idx += 1;
//         item
//     }
// }

fn get_round_robin<'a, T>(v: &'a Vec<T>, mut state: usize) -> (&'a T, usize) {
    let item = v.get(state % v.len()).unwrap();
    state += 1;
    (item, state)
}

// Defined in challenge spec
const MAX_STATIONS: usize = 10000;
const MAX_STATION_NAME_SIZE: usize = 100;
// 5 bytes for two digit float number with a single fractional digit and `;` character
// idea to divide file: pad each line up to MAX_LINE_SIZE bytes
const MAX_LINE_SIZE: usize = MAX_STATION_NAME_SIZE + 5;
const NUM_CONSUMERS: usize = 4;

fn main() -> io::Result<()> {
    // won't accept non-utf-8 args
    let args: Vec<String> = env::args().collect();
    let file_name = match args.get(2).clone() {
        Some(fname) => fname,
        None => "sample.txt",
    };
    // let station_map: [StationData; MAX_STATIONS] = [StationData; MAX_STATIONS];

    println!("Reading from {:}", file_name);

    let f = File::open(file_name)?;
    let buf = &mut BufReader::new(f);

    // works, but is memory intensive
    // Memory limited implementation, but very fast IO

    let lines_to_buff: usize = 1000;

    let mut tx_channels = Vec::new();
    let mut rx_channels = Vec::new();
    for _ in 0..NUM_CONSUMERS {
        let (tx, rx): (Sender<String>, Receiver<String>) = mpsc::channel();
        tx_channels.push(tx);
        rx_channels.push(rx);
    }
    
    thread::scope(|s|{
        s.spawn(move || {
            let mut state: usize = 0;
            let mut line_buff = String::with_capacity((MAX_LINE_SIZE+2)*lines_to_buff);
            let mut read_lines: usize = 0;
            loop {
                let bytes_read = buf.read_line(&mut line_buff).unwrap();
                read_lines += 1;
                if read_lines % lines_to_buff == 0 || bytes_read == 0 {
                    let (tx, new_state) = get_round_robin(&tx_channels, state);
                    state = new_state;
                    tx.send(line_buff.clone()).unwrap();
                    line_buff.clear();
                }
                if bytes_read == 0 {
                    break;
                }
            }
        });
        let mut handlers = Vec::new();
        for _ in 0..NUM_CONSUMERS {
            let rx = rx_channels.pop().unwrap();
            let h = s.spawn(move || {
                let mut station_map: BTreeMap<String, StationData> = BTreeMap::new();
                loop {
                    // unlocks after reading
                    let line_buff_res = {
                        rx.recv()
                    };
                    if let Ok(line_buff) = line_buff_res {
                        for line in StationData::parse_line_buff(&line_buff) {
                            // let fmt_line = &line[0..line.len()-1]; // remove newline
                            let (name, temp) = StationData::parse_data(&line);
                            match station_map.get_mut(&name) {
                                Some(station) => station.update_from(temp),
                                None => {
                                    station_map.insert(name, StationData::new(temp));
                                }
                            };
                        }
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


    // Slow: allocates a string for each line :/
    // for l in lines_reader {
    //     if let Ok(line) = l {
    //         // let (name, data) = StationData::from_string(line);
    //         let (name, temp) = StationData::parse_data(&line);
    //         match station_map.get_mut(&name) {
    //             Some(station) => station.update_from(temp),
    //             None => {
    //                 station_map.insert(name, StationData::new(temp));
    //             }
    //         };
    //     }
    // }

    println!("finished reading");

    Ok(())
}
