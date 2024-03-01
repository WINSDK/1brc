use memchr::memchr;

/// Splits a buffer into `n` chunks at newline boundaries.
///
/// # Arguments
///
/// * `buffer` - The input buffer to split.
/// * `n` - The number of chunks to split the buffer into.
///
/// # Returns
///
/// A vector of byte slices representing the chunks.
pub fn into_chunks(input: &str, n: usize) -> Vec<&str> {
    let mut chunks = Vec::new();
    let mut start = 0;

    if input.is_empty() || n == 0 {
        return chunks;
    }

    let chunk_size = input.len() / n;

    while start < input.len() {
        let mut end = std::cmp::min(start + chunk_size, input.len());

        end = match memchr(b'\n', &input.as_bytes()[end..]) {
            Some(off) => end + off + 1,
            None => input.len()
        };

        chunks.push(&input[start..end]);

        if end >= input.len() - 1 {
            break;
        }

        start = end;
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::into_chunks;

    #[test]
    fn empty() {
        let buffer = "";
        let chunks = into_chunks(buffer, 2);
        assert_eq!(chunks, Vec::<&str>::new());
    }

    #[test]
    fn one() {
        let buffer = "\n";
        let chunks = into_chunks(buffer, 2);
        assert_eq!(chunks, vec!["\n"]);

        let buffer = "\n";
        let chunks = into_chunks(buffer, 1);
        assert_eq!(chunks, vec!["\n"]);

        let buffer = "Kunming;19.8";
        let chunks = into_chunks(buffer, 2);
        assert_eq!(chunks, vec!["Kunming;19.8"]);
    }

    #[test]
    fn evenly_divisible_chunks() {
        let buffer = "Row1\nRow2\nRow3\nRow4\n";
        let chunks = into_chunks(buffer, 2);
        assert_eq!(chunks, vec!["Row1\nRow2\nRow3\n", "Row4\n"]);
    }

    #[test]
    fn not_evenly_divisible_chunks() {
        let buffer = "Row1\nRow2\nRow3\nRow4\nRow5\n";
        let chunks = into_chunks(buffer, 2);
        assert_eq!(chunks, vec!["Row1\nRow2\nRow3\n", "Row4\nRow5\n"]);
    }

    #[test]
    fn more_chunks_than_rows() {
        let buffer = "1\n2\n3\n";
        let chunks = into_chunks(buffer, 5);
        assert_eq!(chunks, vec!["1\n", "2\n", "3\n"]);
    }
}
