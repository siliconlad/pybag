use pybag_rs::mcap::zerocopy::FastMcapReader;
use std::env;
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: bench <mcap_file> [iterations]");
        std::process::exit(1);
    }

    let path = &args[1];
    let iterations: usize = if args.len() > 2 {
        args[2].parse().unwrap_or(100)
    } else {
        100
    };

    // Open file once
    let reader = FastMcapReader::open(path).expect("Failed to open file");

    // Warm up and count messages
    let mut msg_count = 0;
    for msg in reader.iter_messages() {
        std::hint::black_box(&msg);
        msg_count += 1;
    }

    println!("File: {}", path);
    println!("Messages: {}", msg_count);
    println!("Iterations: {}", iterations);

    // Benchmark iterator (file already open)
    let mut total_time_iter = std::time::Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        let mut count = 0;
        for msg in reader.iter_messages() {
            std::hint::black_box(&msg);
            count += 1;
        }
        std::hint::black_box(count);
        total_time_iter += start.elapsed();
    }

    let avg_time_iter_us = total_time_iter.as_nanos() as f64 / iterations as f64 / 1000.0;
    let throughput_iter = msg_count as f64 / (total_time_iter.as_nanos() as f64 / iterations as f64 / 1_000_000_000.0);

    println!("\n=== Rust MCAP Benchmark (Iterator, file pre-opened) ===");
    println!("Average time: {:.2} µs", avg_time_iter_us);
    println!("Throughput: {:.2} msg/s ({:.2} M/s)", throughput_iter, throughput_iter / 1_000_000.0);

    // Benchmark callback-based counting (file already open)
    let mut total_time_cb = std::time::Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        let count = reader.for_each_message(|msg| {
            std::hint::black_box(&msg);
        }).expect("Failed to iterate");
        std::hint::black_box(count);
        total_time_cb += start.elapsed();
    }

    let avg_time_cb_us = total_time_cb.as_nanos() as f64 / iterations as f64 / 1000.0;
    let throughput_cb = msg_count as f64 / (total_time_cb.as_nanos() as f64 / iterations as f64 / 1_000_000_000.0);

    println!("\n=== Rust MCAP Benchmark (Callback, file pre-opened) ===");
    println!("Average time: {:.2} µs", avg_time_cb_us);
    println!("Throughput: {:.2} msg/s ({:.2} M/s)", throughput_cb, throughput_cb / 1_000_000.0);
}
