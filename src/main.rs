use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::io;
use std::rc::Rc;

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

    fn parse_data<'a>(raw: &str) -> (String, f64) {
        let (name, temp) = raw.split_once(";").unwrap();
        (name.to_owned(), temp.parse::<f64>().unwrap())
    }
}

// Defined in challenge spec
const MAX_STATIONS: usize = 10000;
const MAX_STATION_NAME_SIZE: usize = 100;
// 5 bytes for two digit float number with a single fractional digit and `;` character
// idea to divide file: pad each line up to MAX_LINE_SIZE bytes
const MAX_LINE_SIZE: usize = MAX_STATION_NAME_SIZE + 5;

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

    let mut station_map: BTreeMap<String, StationData> = BTreeMap::new();

    // avoid allocating a String for each line. The iterator has that limitation
    let mut line = String::with_capacity(MAX_LINE_SIZE);
    loop {
        line.clear();
        buf.read_line(&mut line)?;
        if line.is_empty() {
            break;
        }
        // remove newline
        let fmt_line = &line[0..line.len()-1];
        let (name, temp) = StationData::parse_data(&fmt_line);
        match station_map.get_mut(&name) {
            Some(station) => station.update_from(temp),
            None => {
                station_map.insert(name, StationData::new(temp));
            }
        };
    }

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

    {
        let mut stdout = io::stdout().lock();
        stdout.write(b"{")?;
        for (k, v) in station_map.into_iter() {
            // ("{}={}/{}/{}", k, v.min_temp, v.mean_temp, v.max_temp)
            write!(
                stdout,
                "{}={}/{}/{}, ",
                k, v.min_temp, v.mean_temp, v.max_temp
            )?;
        }
        stdout.write(b"}")?;
    }

    Ok(())
}
