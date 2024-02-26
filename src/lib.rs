mod tests;

use std::{io::Write, collections::{HashMap, hash_map::Entry}};
use memchr::memchr;

struct Rows<'a> {
    input: &'a str,
    start: usize,
}

impl<'a> Rows<'a> {
    fn new(input: &'a str) -> Self {
        Rows { input, start: 0 }
    }
}

impl<'a> Iterator for Rows<'a> {
    type Item = &'a str;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.input.len() {
            return None;
        }

        let input_bytes = &self.input.as_bytes()[self.start..];
        if let Some(end) = memchr(b'\n', input_bytes) {
            let line = &self.input[self.start..self.start + end];
            // Move past the current line, including the newline character.
            self.start += end + 1;
            Some(line)
        } else {
            // This case handles the last line if it doesn't end with a newline.
            let line = &self.input[self.start..];
            // Move past the end to stop the iteration.
            self.start = self.input.len();
            Some(line)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
struct Record {
    min: f32,
    sum: f32,
    max: f32,
    count: usize,
}

impl Record {
    #[allow(dead_code)]
    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += 1;
    }

    fn add(&mut self, value: f32) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
        self.count += 1;
    }

    fn new(value: f32) -> Self {
        Self {
            min: value,
            max: value,
            sum: value,
            count: 1,
        }
    }

    fn avg(&self) -> f32 {
        // Kinda scuffed method of avoiding rounding errors
        let mean = self.sum / self.count as f32;
        (mean * 10.0).round() / 10.0
    }

    fn write(&self, out: &mut Vec<u8>) {
        let min = self.min;
        let max = self.max;
        let avg = self.avg();

        write!(out, "{min:.1}/{avg:.1}/{max:.1}").unwrap();
    }
}

fn parse(input: &str) -> String {
    let input = input.strip_suffix("\n").unwrap_or(input);
    let mut map = HashMap::new();

    for row in Rows::new(input) {
        let separator = memchr(b';', row.as_bytes()).expect("Missing seperator.");
        let (city, sample) = (&row[..separator], &row[separator + 1..]);
        let sample = sample.parse::<f32>().unwrap();

        match map.entry(city) {
            Entry::Vacant(slot) => {
                slot.insert(Record::new(sample));
            }
            Entry::Occupied(record) => record.into_mut().add(sample),
        }
    }

    let mut entries: Vec<(&str, Record)> = map.into_iter().collect();
    entries.sort_unstable_by_key(|&(city, _)| city);

    let mut out: Vec<u8> = Vec::new();
    out.push(b'{');

    for (idx, (city, record)) in entries.iter().enumerate() {
        out.extend_from_slice(city.as_bytes());
        out.push(b'=');
        record.write(&mut out);

        if idx + 1 != entries.len() {
            out.extend_from_slice(b", ");
        }
    }

    out.extend_from_slice(b"}\n");
    String::from_utf8(out).unwrap()
}

pub fn parse_from_str(input: &str) -> String {
    parse(input)
}

pub fn parse_from_path(path: &str) -> String {
    let file = std::fs::File::open(path).expect("Failed to open file.");

    let data = unsafe { memmap2::Mmap::map(&file) }.expect("Failed to create memory map.");
    let data = &*data;
    let data = std::str::from_utf8(data).expect("Invalid utf-8 input.");

    parse(data)
}

pub fn parse_and_print(path: &str) {
    todo!("{path}")
}
