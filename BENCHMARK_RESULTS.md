# Schema Compilation Benchmark: String-based vs AST-based

## Summary

This document presents the results of benchmarking two approaches to ROS 2 schema compilation in PyBag:
1. **String-based**: Current implementation using string concatenation and `exec()`
2. **AST-based**: New implementation using Python's `ast` module

## Benchmark Results

### Compilation Time (1000 iterations)

| Schema Type | String-based | AST-based | Speedup |
|-------------|--------------|-----------|---------|
| Simple      | 359 µs       | 534 µs    | 0.67x (String 1.49x faster) |
| Medium      | 588 µs       | 997 µs    | 0.59x (String 1.69x faster) |
| Complex     | 570 µs       | 1008 µs   | 0.57x (String 1.77x faster) |

### Runtime Performance (10000 iterations)

| Schema Type | String-based | AST-based | Speedup |
|-------------|--------------|-----------|---------|
| Simple      | 1.19 µs      | 1.20 µs   | 0.99x (essentially equal) |
| Medium      | 2.91 µs      | 2.90 µs   | 1.00x (essentially equal) |
| Complex     | 4.79 µs      | 4.69 µs   | 1.02x (AST 2% faster) |

## Analysis

### Compilation Time
The string-based approach is **1.5-1.8x faster** at compilation time. This is likely because:
- String concatenation and formatting is highly optimized in Python
- Building AST nodes involves more object creation and manipulation
- The AST must be fixed (locations) and compiled to bytecode

However, compilation happens **only once per schema** and results are cached, so this is a one-time cost during initialization.

### Runtime Performance
The runtime performance is **nearly identical** between both approaches, with AST being slightly faster (up to 2%) for complex schemas. This makes sense because:
- Both approaches produce similar bytecode after compilation
- The generated functions perform the same operations
- Minor differences may be due to slightly different code patterns

### Trade-offs

**String-based (Current Implementation)**
- ✅ Faster compilation (1.5-1.8x)
- ✅ Simpler implementation (fewer lines of code)
- ❌ Less maintainable (string manipulation is error-prone)
- ❌ Harder to debug (generated code is strings)
- ❌ No static analysis of generated code

**AST-based (New Implementation)**
- ✅ More maintainable (structured code generation)
- ✅ Easier to debug (AST nodes are inspectable)
- ✅ Better for static analysis
- ✅ Equivalent or slightly better runtime performance
- ❌ Slower compilation (1.5-1.8x)

## Conclusion

The AST-based approach does **not provide a performance improvement** in terms of compilation time, which was the original hypothesis. However, it does offer other benefits:

1. **Code maintainability**: AST nodes are more structured and less error-prone than string manipulation
2. **Runtime performance**: Equivalent or slightly better (up to 2% faster for complex schemas)
3. **Debugging**: Easier to inspect and debug generated code
4. **Compilation time impact**: Minimal in practice since schemas are compiled once and cached

### Recommendation

**Keep the string-based implementation** for now, as:
- Compilation time is already fast (< 1ms per schema)
- The one-time compilation cost is negligible in production
- The current implementation is well-tested and stable

The AST-based implementation could be considered in the future if:
- Maintenance of the compiler becomes more complex
- Better debugging/inspection of generated code is needed
- The compilation time difference becomes negligible with Python optimizations

## Test Environment

- Python 3.11.14
- Platform: Linux
- PyBag version: 0.5.0
- Benchmark iterations: 1000 for compilation, 10000 for runtime

## Schema Definitions

### Simple Schema
- 3 float64 fields (x, y, z)

### Medium Schema
- 1 uint64 (timestamp)
- 6 float fields (x, y, z, vx, vy, vz)
- 1 uint32 (id)
- 1 bool (active)

### Complex Schema
- 1 uint64 (timestamp)
- 3 fixed-size arrays of float64[3] (position, velocity, acceleration)
- 1 fixed-size array of float64[36] (covariance)
- 1 dynamic sequence of uint32 (labels)
