#!/bin/bash
# Benchmark comparison script: Zig vs Rust MCAP readers
#
# Prerequisites:
# - Zig compiler installed (zig version >= 0.13.0)
# - Rust/Cargo installed
# - pybag-rs and pybag-zig directories present

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RUST_DIR="$PROJECT_DIR/pybag-rs"
ZIG_DIR="$PROJECT_DIR/pybag-zig"
TEST_FILE="$PROJECT_DIR/benchmarks/test_bench.mcap"

echo "=== Zig vs Rust MCAP Reader Benchmark ==="
echo ""

# Check for zig
if ! command -v zig &> /dev/null; then
    echo "ERROR: Zig compiler not found. Please install Zig 0.13.0+"
    echo "Download from: https://ziglang.org/download/"
    exit 1
fi

echo "Zig version: $(zig version)"
echo "Rust version: $(rustc --version)"
echo ""

# Build Rust library
echo "Building Rust implementation..."
cd "$RUST_DIR"
cargo build --release --quiet

# Build Zig library
echo "Building Zig implementation..."
cd "$ZIG_DIR"
zig build -Doptimize=ReleaseFast

# Create test file if needed (using Rust writer)
if [ ! -f "$TEST_FILE" ]; then
    echo "Creating test MCAP file with 10000 messages..."
    cd "$RUST_DIR"
    # We'll use the benchmark code to create the file
    cargo run --release --quiet -- create-test "$TEST_FILE" 10000
fi

echo ""
echo "=== Running Benchmarks ==="
echo ""

# Run Zig benchmark
echo "--- Zig Benchmark ---"
cd "$ZIG_DIR"
./zig-out/bin/bench "$TEST_FILE" 100

echo ""

# Run Rust benchmark
echo "--- Rust Benchmark ---"
cd "$RUST_DIR"
cargo bench --bench mcap_comparison 2>&1 | grep -E "(pybag_rs_fast|time:|thrpt:)" | head -20

echo ""
echo "=== Benchmark Complete ==="
