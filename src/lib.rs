mod split;
mod tests;

use hashbrown::hash_table::Entry;
use memchr::memchr;

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::mpsc;

type Map<'a> = hashbrown::HashTable<(u64, Record<'a>)>;

#[inline(always)]
fn hack_map_entry<'map, 'record>(
    map: &'map mut Map<'record>,
    hash: u64,
) -> Entry<'map, (u64, Record<'record>)> {
    // Intentionally use wrong eq operator (instead of city name) for better performances.
    // If there are multiple city with same hash, they will collide.
    map.entry(hash, |(x, _)| *x == hash, |(x, _)| *x)
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
    min: f32,
    sum: f32,
    max: f32,
    count: usize,
}

impl<'a> Record<'a> {
    fn new(value: f32, city: &'a str) -> Self {
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
    fn add(&mut self, value: f32) {
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
        let min = self.min;
        let max = self.max;
        let avg = {
            // Kinda scuffed method of avoiding rounding errors
            let mean = self.sum / self.count as f32;
            (mean * 10.0).round() / 10.0
        };

        write!(out, "{min:.1}/{avg:.1}/{max:.1}").unwrap();
    }
}

#[inline(always)]
fn parse_temperature(input: &[u8]) -> f32 {
    const MAGIC_MULTIPLIER: u64 = 100 * 0x1000000 + 10 * 0x10000 + 1;
    const DOT_BITS: u64 = 0x10101000;

    #[inline(always)]
    fn dot(n: u64) -> u64 {
        (!n & DOT_BITS).trailing_zeros() as u64
    }

    // Function to parse the temperature from a u64 word, given the dot position
    fn value(w: u64, dot: u64) -> i64 {
        let signed = (!w).wrapping_shl(59).wrapping_shr(63);
        let mask = !(signed & 0xFF);
        let digits = ((w & mask) << (28 - dot)) & 0x0F000F0F00;
        let abs = digits.wrapping_mul(MAGIC_MULTIPLIER) >> 32 & 0x3FF;
        ((abs as i64) ^ (signed as i64)) - (signed as i64)
    }

    let n = unsafe { std::ptr::read(input.as_ptr() as *const u32) as u64 };
    let dot = dot(n);
    return (value(n, dot) as f64 / 10.0) as f32;

    // let neg = input[0] == b'-';
    // let len = input.len();

    // let (d1, d2, d3) = match (neg, len) {
    //     (false, 3) => (0, input[0] - b'0', input[2] - b'0'),
    //     (false, 4) => (input[0] - b'0', input[1] - b'0', input[3] - b'0'),
    //     (true, 4) => (0, input[1] - b'0', input[3] - b'0'),
    //     (true, 5) => (input[1] - b'0', input[2] - b'0', input[4] - b'0'),
    //     _ => unreachable!(),
    // };

    // let int = (d1 as i16 * 100) + (d2 as i16 * 10) + (d3 as i16);
    // let int = if neg { -int } else { int };

    // int as f32 * 0.1
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
                let mut local_map = Map::with_capacity(1024);

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

                    hack_map_entry(&mut local_map, hash)
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
