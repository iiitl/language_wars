use fxhash::FxHashSet;
use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};

// // High DISK USAGE DONT USE CRASHES ..
// const PARTITIONS: usize = 512;
// const CHUNK_SIZE: usize = 512 * 1024 * 1024;
// Low DISK USAGE
const PARTITIONS: usize = 328;
const CHUNK_SIZE: usize = 256 * 1024 * 1024;

fn hash_partition(word: &str) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut hasher = DefaultHasher::new();
    hasher.write(word.as_bytes());
    (hasher.finish() as usize) % PARTITIONS
}

fn main() -> std::io::Result<()> {
    let input_dir = "test_cases";
    let output_file = "unique_words_sorted.txt";

    let writers: Vec<_> = (0..PARTITIONS)
        .map(|i| {
            Arc::new(RwLock::new(BufWriter::new(
                File::create(format!("partition_{i}.txt")).unwrap(),
            )))
        })
        .collect();

    let files: Vec<_> = fs::read_dir(input_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|p| p.is_file())
        .collect();

    files.par_iter().for_each(|path| {
        if let Ok(metadata) = fs::metadata(path) {
            let file_size = metadata.len() as usize;
            let num_chunks = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
            let path = path.clone();

            (0..num_chunks).into_par_iter().for_each(|chunk_idx| {
                if let Ok(mut file) = File::open(&path) {
                    let start = chunk_idx * CHUNK_SIZE;
                    let chunk_len = CHUNK_SIZE.min(file_size - start);

                    if file.seek(SeekFrom::Start(start as u64)).is_ok() {
                        // If not the first chunk, skip to the next whitespace
                        if chunk_idx != 0 {
                            let mut byte = [0u8; 1];
                            while let Ok(1) = file.read(&mut byte) {
                                if byte[0].is_ascii_whitespace() {
                                    break;
                                }
                            }
                        }

                        let mut buffer = Vec::with_capacity(chunk_len + 64);
                        let mut read_buffer = vec![0u8; chunk_len];
                        if file.read_exact(&mut read_buffer).is_ok() {
                            buffer.extend(read_buffer);

                            // Read extra bytes to complete the last word
                            let mut byte = [0u8; 1];
                            while let Ok(1) = file.read(&mut byte) {
                                buffer.push(byte[0]);
                                if byte[0].is_ascii_whitespace() {
                                    break;
                                }
                            }

                            let content = String::from_utf8_lossy(&buffer);
                            for line in content.lines() {
                                for word in line.split_whitespace() {
                                    let word = word.to_lowercase();
                                    let idx = hash_partition(&word);
                                    if let Ok(mut w) = writers[idx].write() {
                                        let _ = writeln!(w, "{word}");
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
    });

    for w in &writers {
        let _ = w.write().unwrap().flush();
    }

    (0..PARTITIONS).into_par_iter().for_each(|i| {
        let mut set: FxHashSet<String> = FxHashSet::default();
        if let Ok(file) = File::open(format!("partition_{i}.txt")) {
            let reader = BufReader::new(file);
            for line in reader.lines().flatten() {
                set.insert(line);
            }
        }

        let mut sorted: Vec<_> = set.into_iter().collect();
        sorted.sort_unstable();
        let mut out = BufWriter::new(File::create(format!("partition_{i}.bin")).unwrap());

        for word in &sorted {
            let bytes = word.as_bytes();
            let len = bytes.len() as u32;
            let _ = out.write_all(&len.to_le_bytes());
            let _ = out.write_all(bytes);
        }
    });

    let mut heap = BinaryHeap::new();
    let mut readers: Vec<Option<BufReader<File>>> = (0..PARTITIONS).map(|_| None).collect();

    for i in 0..PARTITIONS {
        if let Ok(file) = File::open(format!("partition_{i}.bin")) {
            let mut reader = BufReader::new(file);
            if let Some(word) = read_next_word(&mut reader)? {
                heap.push(Reverse((word.clone(), i)));
                readers[i] = Some(reader);
            }
        }
    }

    let mut out = BufWriter::new(File::create(output_file)?);
    let mut last_written: Option<String> = None;

    while let Some(Reverse((word, idx))) = heap.pop() {
        if last_written.as_ref().map_or(true, |w| w != &word) {
            writeln!(out, "{word}")?;
            last_written = Some(word.clone());
        }

        if let Some(reader) = &mut readers[idx] {
            if let Some(next_word) = read_next_word(reader)? {
                heap.push(Reverse((next_word, idx)));
            }
        }
    }

    out.flush()?;

    (0..PARTITIONS).into_par_iter().for_each(|i| {
        let _ = fs::remove_file(format!("partition_{i}.txt"));
        let _ = fs::remove_file(format!("partition_{i}.bin"));
    });

    Ok(())
}

fn read_next_word<R: BufRead>(reader: &mut R) -> std::io::Result<Option<String>> {
    let mut len_buf = [0u8; 4];
    if reader.read_exact(&mut len_buf).is_err() {
        return Ok(None); // EOF
    }
    let len = u32::from_le_bytes(len_buf);
    let mut word_buf = vec![0u8; len as usize];
    reader.read_exact(&mut word_buf)?;
    Ok(Some(String::from_utf8_lossy(&word_buf).into_owned()))
}
