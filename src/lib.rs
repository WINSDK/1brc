mod split;
mod tests;

use memchr::memchr;
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::mpsc;

#[inline(always)]
fn parse_temperature(input: &[u8]) -> i32 {
    const MAGIC_MULTIPLIER: i64 = 100 * 0x1000000 + 10 * 0x10000 + 1;
    const DOT_BITS: i64 = 0x10101000;

    #[inline(always)]
    fn dot(n: i64) -> i64 {
        (!n & DOT_BITS).trailing_zeros() as i64
    }

    #[inline(always)]
    fn value(w: i64, dot: i64) -> i64 {
        let signed = (!w).wrapping_shl(59).wrapping_shr(63);
        let mask = !(signed & 0xFF);
        let digits = ((w & mask) << (28 - dot)) & 0x0F000F0F00;
        let abs = digits.wrapping_mul(MAGIC_MULTIPLIER) >> 32 & 0x3FF;
        ((abs ^ signed) - signed) as i64
    }

    let n = unsafe { std::ptr::read(input.as_ptr() as *const i64) };
    value(n, dot(n)) as i32
}

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

        let input_bytes = unsafe { self.input.as_bytes().get_unchecked(self.start..) };
        if let Some(end) = memchr(b'\n', input_bytes) {
            let line = unsafe { self.input.get_unchecked(self.start..self.start + end) };
            // Move past the current line, including the newline character.
            self.start += end + 1;
            Some(line)
        } else {
            // This case handles the last line if it doesn't end with a newline.
            let line = unsafe { self.input.get_unchecked(self.start..) };
            // Move past the end to stop the iteration.
            self.start = self.input.len();
            Some(line)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
struct Record<'a> {
    city: &'a str,
    min: i32,
    sum: i32,
    max: i32,
    count: usize,
}

impl<'a> Record<'a> {
    fn new(value: i32, city: &'a str) -> Self {
        Self {
            city,
            min: value,
            max: value,
            sum: value,
            count: 1,
        }
    }

    #[inline(always)]
    fn merge(&mut self, other: Self) {
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        self.sum += other.sum;
        self.count += other.count;
    }

    #[inline(always)]
    fn add(&mut self, value: i32) {
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.sum += value;
        self.count += 1;
    }

    fn write(&self, out: &mut Vec<u8>) {
        let min = self.min as f32 / 10.0;
        let max = self.max as f32 / 10.0;
        let avg = {
            // Kinda scuffed method of avoiding rounding errors.
            let mean = (self.sum as f32 / 10.0) / self.count as f32;
            (mean * 10.0).round() / 10.0
        };

        write!(out, "{min:.1}/{avg:.1}/{max:.1}").unwrap();
    }
}

pub fn parse_from_str(input: &str) -> String {
    let input = input.strip_suffix("\n").unwrap_or(input);

    let thread_count = std::thread::available_parallelism()
        .map(|x| x.get())
        .unwrap_or(4);

    let (tx, rx) = mpsc::channel();
    let chunks = split::into_chunks(input, thread_count * 4);
    let chunk_count = chunks.len();

    std::thread::scope(move |s| {
        for chunk in chunks {
            let tx = tx.clone();
            s.spawn(move || {
                let mut local_map = hashbrown::HashTable::<(u64, Record)>::with_capacity(1024);

                for row in Rows::new(chunk) {
                    let separator = memchr(b';', row.as_bytes()).expect("Missing seperator.");
                    let (city, sample) = unsafe {
                        (
                            row.get_unchecked(..separator),
                            row.get_unchecked(separator + 1..),
                        )
                    };

                    let hash = {
                        use std::hash::Hasher;

                        let mut hasher = rustc_hash::FxHasher::default();
                        hasher.write(city.as_bytes());
                        hasher.write_u8(0xff);
                        hasher.finish()
                    };

                    let sample = parse_temperature(sample.as_bytes());

                    // very dumb hack that avoids comparisons but may cause collisions
                    local_map.entry(hash, |(x, _)| *x == hash, |(x, _)| *x)
                        .and_modify(|(_, existing)| existing.add(sample))
                        .or_insert_with(|| (hash, Record::new(sample, city)));
                }

                tx.send(local_map).unwrap();
            });
        }
    });

    let mut map = BTreeMap::default();
    for _ in 0..chunk_count {
        let local_map = rx.recv().unwrap();

        for (_, record) in local_map {
            map.entry(record.city)
                .and_modify(|existing: &mut Record| existing.merge(record))
                .or_insert(record);
        }
    }

    let mut out = Vec::new();
    out.push(b'{');

    for (idx, (_, record)) in map.iter().enumerate() {
        out.extend_from_slice(record.city.as_bytes());
        out.push(b'=');
        record.write(&mut out);

        if idx + 1 != map.len() {
            out.extend_from_slice(b", ");
        }
    }

    out.extend_from_slice(b"}\n");
    unsafe { String::from_utf8_unchecked(out) }
}

pub fn parse_from_path(path: &str) -> String {
    let file = std::fs::File::open(path).expect("Failed to open file.");

    let data = unsafe { memmap2::Mmap::map(&file) }.expect("Failed to create memory map.");
    let data = &*data;
    let data = unsafe { std::str::from_utf8_unchecked(data) };

    parse_from_str(data)
}
