extern crate aws_lambda as lambda;
extern crate chrono;

extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate lazy_static;
extern crate regex;
extern crate rayon;

use std::fmt;
use chrono::prelude::*;
use regex::Regex;
use rayon::prelude::*;

use lambda::event::Base64Data;
use lambda::event::firehose::{KinesisFirehoseEvent, KinesisFirehoseEventRecord, KinesisFirehoseResponse, KinesisFirehoseResponseRecord};

lazy_static! {
    static ref RE: Regex = Regex::new(r#"^([\d.]+) (\S+) (\S+) \[([\w:/]+\s[\+\-]\d{2}:?\d{2}){0,1}\] "(.+?)" (\d{3}) (\d+)"#).unwrap();
}

fn main() {
    lambda::start(|input: KinesisFirehoseEvent| {
        Ok(my_handler(input))
    })
}

#[derive(Serialize, Deserialize, Debug)]
struct AccessLog {
    host: String,
    ident: String,
    authuser: String,
    #[serde(rename = "@timestamp")]
    timestamp: String,
    #[serde(rename = "@timestamp_utc")]
    timestamp_utc: String,
    request: String,
    response: u32,
    bytes: u32,
}

#[derive(Debug)]
enum LogError {
    RegexParseError,
    UTF8Error(std::string::FromUtf8Error),
    DateTimeParseError(chrono::ParseError),
    IntError(std::num::ParseIntError),
    JsonError(serde_json::Error)
}

impl From<std::string::FromUtf8Error> for LogError {
    fn from(err: std::string::FromUtf8Error) -> LogError {
        LogError::UTF8Error(err)
    }
}

impl From<chrono::ParseError> for LogError {
    fn from(err: chrono::ParseError) -> LogError {
        LogError::DateTimeParseError(err)
    }
}

impl From<std::num::ParseIntError> for LogError {
    fn from(err: std::num::ParseIntError) -> LogError {
        LogError::IntError(err)
    }
}

impl From<serde_json::Error> for LogError {
    fn from(err: serde_json::Error) -> LogError {
        LogError::JsonError(err)
    }
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            LogError::RegexParseError => fmt::Display::fmt(self, f),
            LogError::UTF8Error(ref err) => fmt::Display::fmt(err, f),
            LogError::DateTimeParseError(ref err) => fmt::Display::fmt(err, f),
            LogError::IntError(ref err) => fmt::Display::fmt(err, f),
            LogError::JsonError(ref err) => fmt::Display::fmt(err, f),
        }
    }
}

impl std::error::Error for LogError {
    fn description(&self) -> &str {
        match *self {
            LogError::RegexParseError => "FAIL. unmatched pattern.",
            LogError::UTF8Error(ref err) => err.description(),
            LogError::DateTimeParseError(ref err) => err.description(),
            LogError::IntError(ref err) => err.description(),
            LogError::JsonError(ref err) => err.description(),
        }
    }
}

fn apache_log2json(s: &str) -> Result<serde_json::Value, LogError> {
    let xs = RE.captures(s).ok_or(LogError::RegexParseError)?;

    let time =
        DateTime::parse_from_str(&xs[4], "%d/%b/%Y:%H:%M:%S %:z")
            .or(DateTime::parse_from_str(&xs[4], "%d/%b/%Y:%H:%M:%S %z"))?;
    xs[6].parse::<u32>()?;

    let log =  AccessLog {
        host: xs[1].to_owned(),
        ident: xs[2].to_owned(),
        authuser: xs[3].to_owned(),
        timestamp: time.to_rfc3339(),
        timestamp_utc: time.with_timezone(&Utc).to_rfc3339(),
        request: xs[5].to_owned(),
        response: xs[6].parse::<u32>()?,
        bytes: xs[7].parse::<u32>()?,
    };
    serde_json::to_value(log).map_err(|e| LogError::JsonError(e))
}

fn transform_data(data: Vec<u8>) -> std::result::Result<Vec<u8>, LogError> {
    let s = String::from_utf8(data)?;

    let r = apache_log2json(s.as_str())?;

    serde_json::to_vec(&r).map_err(|e| LogError::JsonError(e))
}

#[test]
fn transform_data_test() {
    let data = r#"7.248.7.119 - - [14/Dec/2017:22:16:45 +09:00] "GET /explore" 200 9947 "-" "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:8.5) Gecko/20100101 Firefox/8.5.1" "#;
    let a = apache_log2json(data).unwrap();

    println!("{}", a);
}

fn transform_record(record: KinesisFirehoseEventRecord) -> KinesisFirehoseResponseRecord {
    let id = record.record_id.clone();
    transform_data(record.data.as_slice().to_vec())
        .map(|x|
            KinesisFirehoseResponseRecord {
                record_id: id.clone(),
                data: Base64Data::new(x),
                result: None,
            }
        )
        .unwrap_or(
            KinesisFirehoseResponseRecord {
                record_id: id,
                data: record.data,
                result: None,
            }
        )
}

fn my_handler(event: KinesisFirehoseEvent) -> KinesisFirehoseResponse {
    KinesisFirehoseResponse {
        records: event.records.into_par_iter()
            .map(|x| transform_record(x))
            .collect::<Vec<KinesisFirehoseResponseRecord>>(),
    }
}