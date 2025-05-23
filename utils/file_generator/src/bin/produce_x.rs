use rand::random_range;
use rand::{distr::Alphanumeric, Rng};
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::process;

const WORD_MIN: usize = 6;
const WORD_MAX: usize = 8;

fn generate_word() -> String {
    let len = random_range(WORD_MIN..=WORD_MAX);
    rand::rng()
        .sample_iter(&Alphanumeric)
        .filter(|c| c.is_ascii_alphabetic())
        .take(len)
        .map(char::from)
        .collect()
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <X in MB>", args[0]);
        process::exit(1);
    }

    let mb: u64 = args[1].parse().expect("Invalid number for MB input");
    let bytes_needed = mb * 1024 * 1024;

    let file_name = format!("test_cases/output_{}.txt", rand::random_range(0..=1000));

    std::fs::create_dir_all("test_cases").expect("Failed to create directory");

    let file = File::create(&file_name).expect("Failed to create file");
    let mut writer = BufWriter::new(file);

    let mut bytes_written = 0;

    while bytes_written < bytes_needed {
        let word = generate_word();
        let word_with_space = format!("{} ", word);
        bytes_written += word_with_space.len() as u64;

        writer
            .write_all(word_with_space.as_bytes())
            .expect("Write failed");
    }

    writer.flush().expect("Flush failed");
    println!("Generated {} MB of word data in {}", mb, file_name);
}
