# pybag-zig

High-performance Zig implementation of the pybag MCAP reader.

## Building

```bash
# Build all targets
zig build

# Build optimized release
zig build -Doptimize=ReleaseFast

# Run benchmarks
zig build bench -- /path/to/file.mcap 100
```

## Usage

### From Zig

```zig
const std = @import("std");
const mcap = @import("mcap.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var reader = try mcap.FastMcapReader.open(gpa.allocator(), "data.mcap");
    defer reader.close();

    // Iterator style
    var iter = reader.iterMessages();
    while (iter.next()) |msg| {
        std.debug.print("Message: channel={} time={}\n", .{msg.channel_id, msg.log_time});
    }

    // Or count messages
    const count = try reader.countMessages();
    std.debug.print("Total messages: {}\n", .{count});
}
```

### From C/Python via FFI

```c
#include <stdint.h>

typedef struct {
    uint16_t channel_id;
    uint32_t sequence;
    uint64_t log_time;
    uint64_t publish_time;
    const uint8_t* data_ptr;
    size_t data_len;
} CMessage;

// Open a file
void* pybag_zig_open(const char* path);

// Close the reader
void pybag_zig_close(void* handle);

// Count messages
int64_t pybag_zig_count_messages(void* handle);

// Iterate with callback
typedef void (*MessageCallback)(const CMessage* msg, void* user_data);
int64_t pybag_zig_for_each_message(void* handle, MessageCallback cb, void* user_data);
```

## Performance

The Zig implementation uses:
- Memory-mapped file I/O for zero-copy reads
- Inline functions for parsing primitives
- No allocations during message iteration
- Direct pointer arithmetic for maximum speed

Expected performance should be competitive with or exceed the Rust implementation due to:
- Zig's lack of hidden runtime costs
- Explicit control over memory layout
- Compile-time optimization opportunities

## Benchmarking vs Rust

To run a comparison benchmark:

```bash
# Create a test MCAP file using the Rust writer
cd ../pybag-rs
cargo run --release --example create_test_file 10000

# Run Zig benchmark
cd ../pybag-zig
zig build bench -- ../test.mcap 100

# Run Rust benchmark
cd ../pybag-rs
cargo bench --bench mcap_comparison
```
