use pybag_rs::mcap::writer::McapWriter;
use pybag_rs::mcap::records::{ChannelRecord, MessageRecord, SchemaRecord};
use std::collections::HashMap;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: create_test <output.mcap> <message_count>");
        std::process::exit(1);
    }

    let path = &args[1];
    let count: usize = args[2].parse().expect("Invalid message count");

    let mut writer = McapWriter::create(
        path,
        "ros2",
        None,  // No chunking for benchmark
        None,  // No compression
    ).expect("Failed to create writer");

    let schema = SchemaRecord {
        id: 1,
        name: "geometry_msgs/msg/Point".to_string(),
        encoding: "ros2msg".to_string(),
        data: b"float64 x\nfloat64 y\nfloat64 z\n".to_vec(),
    };
    writer.write_schema(&schema).expect("Failed to write schema");

    let channel = ChannelRecord {
        id: 1,
        schema_id: 1,
        topic: "/test_point".to_string(),
        message_encoding: "cdr".to_string(),
        metadata: HashMap::new(),
    };
    writer.write_channel(&channel).expect("Failed to write channel");

    // CDR encoded Point: 4 byte header + 3 * 8 bytes = 28 bytes
    let mut cdr_data = vec![0u8; 28];
    cdr_data[0] = 0x00;
    cdr_data[1] = 0x01; // Little endian

    for i in 0..count {
        let x = (i as f64).to_le_bytes();
        let y = ((i * 2) as f64).to_le_bytes();
        let z = ((i * 3) as f64).to_le_bytes();
        cdr_data[4..12].copy_from_slice(&x);
        cdr_data[12..20].copy_from_slice(&y);
        cdr_data[20..28].copy_from_slice(&z);

        let message = MessageRecord {
            channel_id: 1,
            sequence: i as u32,
            log_time: (i as u64) * 1_000_000,
            publish_time: (i as u64) * 1_000_000,
            data: cdr_data.clone(),
        };
        writer.write_message(&message).expect("Failed to write message");
    }

    writer.close().expect("Failed to close writer");
    println!("Created {} with {} messages", path, count);
}
