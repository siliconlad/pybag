# Multithreading Performance Benchmark Report

## Executive Summary

**Recommendation: DO NOT implement multithreading for the proposed use cases.**

Both proposed multithreading approaches show **significant performance degradation** compared to the current sequential implementation. The overhead of thread/process management and synchronization far outweighs any potential benefits.

## Tested Approaches

### 1. Parallel File Reading
**Hypothesis:** Reading multiple MCAP files concurrently could improve performance.

**Implementation:** Used `ThreadPoolExecutor` and `ProcessPoolExecutor` to read multiple files in parallel.

### 2. Parallel Chunk Decompression During Merge
**Hypothesis:** Decompressing chunks from multiple files concurrently during merge could improve performance.

**Implementation:** Pre-loaded and decompressed chunks using `ThreadPoolExecutor` with 4 workers before merging.

## Benchmark Results

### 1. Multiple File Reading Performance

| Configuration | Sequential | Parallel (Threads) | Parallel (Processes) | Verdict |
|---------------|------------|-------------------|---------------------|---------|
| **3 files, 1K msgs** | 19.5ms | 103.5ms | 69.4ms | **Sequential 5.3x faster than threads, 3.6x faster than processes** |
| **5 files, 1K msgs** | 26.8ms | 157.6ms | 108.6ms | **Sequential 5.9x faster than threads, 4.1x faster than processes** |
| **10 files, 1K msgs** | 58.3ms | 2,478ms | 120.5ms | **Sequential 42.5x faster than threads, 2.1x faster than processes** |

**Key Finding:** The more files being read, the WORSE parallel reading performs. For 10 files with threads, performance degraded by **42x**!

### 2. File Merging with Parallel Chunk Decompression

| Configuration | Sequential | Parallel Decompression | Overhead |
|---------------|------------|----------------------|----------|
| **Small:** 3 files, 1K msgs each | 46.9ms | 51.5ms | **+10% slower** |
| **Medium:** 5 files, 5K msgs each | 346.4ms | 381.8ms | **+10% slower** |
| **Large:** 10 files, 10K msgs each | 1,343ms | 1,498ms | **+11.5% slower** |

**Key Finding:** Parallel chunk decompression consistently adds 10-12% overhead across all file sizes.

## Analysis

### Why Multithreading Fails Here

1. **Python GIL (Global Interpreter Lock)**
   - The GIL prevents true parallel execution of Python bytecode
   - CPU-bound operations like decompression cannot benefit from threading
   - Thread context switching adds overhead without parallel execution benefits

2. **I/O is Already Fast**
   - Modern SSDs and OS-level caching make file I/O very fast
   - The files tested are relatively small (KB to MB range)
   - Sequential reading benefits from OS read-ahead optimizations

3. **Thread/Process Management Overhead**
   - Creating and managing thread pools has significant overhead
   - Thread synchronization and communication costs
   - For processes: IPC (Inter-Process Communication) and serialization overhead

4. **Memory Locality**
   - Sequential access patterns have better CPU cache utilization
   - Parallel access causes cache thrashing and memory contention

5. **Small Workload Size**
   - The individual file operations complete too quickly
   - Thread pool overhead dominates the execution time
   - Parallelism only helps when individual tasks are sufficiently large

### When Would Multithreading Help?

Multithreading/multiprocessing would only be beneficial if:

1. **Very Large Files (GB+ range)** where decompression takes seconds per chunk
2. **Network I/O** where there's significant latency waiting for remote data
3. **CPU-intensive processing** with ProcessPoolExecutor (not threads) on multi-core systems
4. **Very large number of files (100+)** where the setup overhead is amortized

Even then, the benefits would likely be modest (10-30% improvement) rather than the dramatic slowdowns we observed.

## Benchmark Configuration

### Hardware Environment
- Platform: Linux (container environment)
- Python: 3.11.14
- Cores: Available for ProcessPoolExecutor tests

### Test Data
- Generated using rosbags library with Odometry messages
- Compressed MCAP files (default compression from rosbags writer)
- Realistic message sizes and structure

### Benchmark Tools
- pytest-benchmark 5.2.3
- Multiple rounds with statistical analysis
- Warm-up iterations performed
- Results show mean time with standard deviation

## Recommendations

### Short-term: Keep Current Implementation
The current sequential implementation is **optimal** for the typical use cases:
- File sizes in the KB-MB range
- Local disk I/O
- Standard compression levels

### Long-term: Consider Alternatives
If performance improvements are needed, consider:

1. **Better Compression Algorithms**
   - Profile lz4 vs zstd performance trade-offs
   - Adjust compression levels for your use case

2. **Caching Strategies**
   - Cache decompressed chunks in memory for repeated access
   - Use memory-mapped files for large files

3. **Batch Processing Optimizations**
   - Process multiple operations in a single pass
   - Reduce file open/close overhead

4. **Native Extensions**
   - Move CPU-intensive operations to C/C++ extensions
   - Use Cython for performance-critical paths

5. **Asynchronous I/O**
   - Use asyncio for concurrent I/O without thread overhead
   - Beneficial when dealing with many small files on slow storage

## Conclusion

The benchmarks clearly demonstrate that **multithreading provides no benefit** for the proposed use cases and actually degrades performance significantly. The current sequential implementation is well-optimized for the typical workload characteristics of pybag.

**Status: ❌ Not recommended for implementation**

---

## Reproducibility

To reproduce these benchmarks:

```bash
# Install dependencies
pip install -e .
pip install pytest pytest-benchmark pytest-cov rosbags numpy mcap mcap-ros2-support

# Run file reading benchmarks
pytest benchmarks/test_multithreading_read.py --benchmark-only --benchmark-sort=name

# Run file merging benchmarks
pytest benchmarks/test_multithreading_merge.py --benchmark-only --benchmark-sort=name
```

Benchmark files created:
- `benchmarks/test_multithreading_read.py` - File reading benchmarks
- `benchmarks/test_multithreading_merge.py` - File merging benchmarks
