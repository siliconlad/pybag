//! Benchmark comparison between pybag_rs and the official mcap Rust library.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs::File;
use std::path::Path;
use tempfile::TempDir;

// Import our implementation
use pybag_rs::mcap::reader::McapReader;
use pybag_rs::mcap::writer::McapWriter;
use pybag_rs::mcap::records::{ChannelRecord, MessageRecord, SchemaRecord};
use pybag_rs::mcap::zerocopy::{FastMcapReader, count_messages_fast};

// Import official mcap library
use mcap;

/// Create a test MCAP file with the specified number of messages.
fn create_test_mcap(path: &Path, message_count: usize) -> std::io::Result<()> {
    let mut writer = McapWriter::create(
        path,
        "ros2",
        Some(1024 * 1024), // 1MB chunks
        None, // No compression for benchmark comparison
    ).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;

    // Create a simple schema for Point messages
    let schema = SchemaRecord {
        id: 1,
        name: "geometry_msgs/msg/Point".to_string(),
        encoding: "ros2msg".to_string(),
        data: b"float64 x\nfloat64 y\nfloat64 z\n".to_vec(),
    };
    writer.write_schema(&schema)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;

    // Create a channel
    let channel = ChannelRecord {
        id: 1,
        schema_id: 1,
        topic: "/test_point".to_string(),
        message_encoding: "cdr".to_string(),
        metadata: std::collections::HashMap::new(),
    };
    writer.write_channel(&channel)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;

    // Write messages with CDR-encoded Point data
    // CDR format: 4 byte header + 3 * 8 bytes (float64 x, y, z) = 28 bytes
    let mut cdr_data = vec![0u8; 28];
    cdr_data[0] = 0x00; // CDR header byte 0
    cdr_data[1] = 0x01; // Little endian flag
    cdr_data[2] = 0x00; // CDR header byte 2
    cdr_data[3] = 0x00; // CDR header byte 3

    for i in 0..message_count {
        // Set x, y, z values (as f64 little-endian)
        let x = (i as f64).to_le_bytes();
        let y = ((i * 2) as f64).to_le_bytes();
        let z = ((i * 3) as f64).to_le_bytes();
        cdr_data[4..12].copy_from_slice(&x);
        cdr_data[12..20].copy_from_slice(&y);
        cdr_data[20..28].copy_from_slice(&z);

        let message = MessageRecord {
            channel_id: 1,
            sequence: i as u32,
            log_time: (i as u64) * 1_000_000, // 1ms apart
            publish_time: (i as u64) * 1_000_000,
            data: cdr_data.clone(),
        };
        writer.write_message(&message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;
    }

    writer.close()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;

    Ok(())
}

/// Read messages using our pybag_rs implementation.
fn read_with_pybag_rs(path: &Path) -> usize {
    let mut reader = McapReader::open(path, false).expect("Failed to open MCAP file");
    let messages = reader
        .messages(None, None, None, true, false)
        .expect("Failed to read messages");
    messages.len()
}

/// Read messages using the official mcap Rust library.
fn read_with_official_mcap(path: &Path) -> usize {
    let file = File::open(path).expect("Failed to open file");
    let mapped = unsafe { memmap2::Mmap::map(&file).expect("Failed to mmap") };

    let mut count = 0;
    for message in mcap::MessageStream::new(&mapped).expect("Failed to create message stream") {
        let _ = black_box(message.expect("Failed to read message"));
        count += 1;
    }
    count
}

/// Read messages using our fast zero-copy implementation (iterator).
fn read_with_pybag_rs_fast(path: &Path) -> usize {
    let reader = FastMcapReader::open(path).expect("Failed to open MCAP file");
    let mut count = 0;
    for msg in reader.iter_messages() {
        let _ = black_box(msg);
        count += 1;
    }
    count
}

/// Read messages using our fast zero-copy implementation (callback).
fn read_with_pybag_rs_fast_callback(path: &Path) -> usize {
    let reader = FastMcapReader::open(path).expect("Failed to open MCAP file");
    reader.for_each_message(|msg| {
        let _ = black_box(msg);
    }).expect("Failed to read messages")
}

/// Count messages using fast path (for chunked files).
fn count_with_pybag_rs_fast(path: &Path) -> usize {
    count_messages_fast(path).expect("Failed to count messages")
}

fn benchmark_reading(c: &mut Criterion) {
    let mut group = c.benchmark_group("mcap_reading");

    // Test with different message counts
    for message_count in [100, 1000, 5000, 10000].iter() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mcap_path = temp_dir.path().join("test.mcap");

        create_test_mcap(&mcap_path, *message_count)
            .expect("Failed to create test MCAP file");

        group.throughput(Throughput::Elements(*message_count as u64));

        group.bench_with_input(
            BenchmarkId::new("pybag_rs", message_count),
            &mcap_path,
            |b, path| {
                b.iter(|| read_with_pybag_rs(black_box(path)))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("official_mcap", message_count),
            &mcap_path,
            |b, path| {
                b.iter(|| read_with_official_mcap(black_box(path)))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("pybag_rs_fast", message_count),
            &mcap_path,
            |b, path| {
                b.iter(|| read_with_pybag_rs_fast(black_box(path)))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("pybag_rs_fast_callback", message_count),
            &mcap_path,
            |b, path| {
                b.iter(|| read_with_pybag_rs_fast_callback(black_box(path)))
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_reading);
criterion_main!(benches);
