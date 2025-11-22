# Message Generation Workflow

This document explains the complete workflow for generating ROS2 message types and pre-compiled encoders/decoders.

## Quick Start

### Generate Pre-compiled Code for Default Packages

```bash
# Generate for Humble (default distro)
python scripts/generate_messages.py --precompile

# Output: src/pybag/precompiled/humble.py
```

This generates pre-compiled encoders/decoders for:
- `builtin_interfaces` (Time, Duration)
- `std_msgs` (Header, String, Int32, etc.)
- `geometry_msgs` (Point, Pose, Transform, etc.)
- `nav_msgs` (Odometry, Path, OccupancyGrid, etc.)
- `sensor_msgs` (Image, LaserScan, PointCloud2, etc.)

### Generate for Specific Packages

```bash
# Only generate for std_msgs and geometry_msgs
python scripts/generate_messages.py \
  --distro humble \
  --precompile \
  --packages std_msgs geometry_msgs
```

### Generate for a Different ROS2 Distro

```bash
# Generate for Iron
python scripts/generate_messages.py \
  --distro iron \
  --precompile \
  --precompiled-output src/pybag/precompiled/iron.py
```

## Detailed Workflow

### 1. Understanding the Scripts

Two scripts are available:

**`scripts/generate_messages.py`** (Recommended)
- Full-featured generation tool
- Supports multiple distros
- Can generate dataclasses AND pre-compiled code
- Selective package generation

**`scripts/precompile_messages.py`** (Legacy)
- Simple script for Humble only
- Generates all standard packages
- Kept for backward compatibility

### 2. Script Options

```
--distro DISTRO           ROS2 distribution (humble, iron, jazzy, etc.)
--dataclasses             Generate message dataclasses
--precompile              Generate pre-compiled encoders/decoders
--packages PKG [...]      Specific packages to process
--output-dir DIR          Output directory for dataclasses
--precompiled-output FILE Output file for pre-compiled code
```

### 3. Common Use Cases

#### Use Case 1: Update Pre-compiled Code After Adding Messages

```bash
# 1. Add new message definitions to src/pybag/ros2/humble/my_package.py
# 2. Regenerate pre-compiled code
python scripts/generate_messages.py --precompile --packages my_package
```

#### Use Case 2: Support a New ROS2 Distribution

```bash
# 1. Create directory
mkdir -p src/pybag/ros2/jazzy

# 2. Copy and adapt message definitions from another distro
cp -r src/pybag/ros2/humble/* src/pybag/ros2/jazzy/

# 3. Generate pre-compiled code
python scripts/generate_messages.py \
  --distro jazzy \
  --precompile \
  --precompiled-output src/pybag/precompiled/jazzy.py
```

#### Use Case 3: Generate Code for Custom Message Package

```bash
# 1. Create message definitions manually
# src/pybag/ros2/humble/my_custom_msgs.py

# 2. Generate pre-compiled code
python scripts/generate_messages.py \
  --distro humble \
  --precompile \
  --packages builtin_interfaces std_msgs my_custom_msgs
```

### 4. Verification

After generating code, verify it works:

```python
# Test pre-compiled functions exist
from pybag import precompiled

decoder = precompiled.get_decoder('std_msgs/msg/Header')
encoder = precompiled.get_encoder('std_msgs/msg/Header')

assert decoder is not None, "Decoder not found"
assert encoder is not None, "Encoder not found"

print("✓ Pre-compiled functions available")
```

Run the benchmark to measure performance:

```bash
python benchmarks/precompile_benchmark.py
```

### 5. Testing

Run the test suite to ensure everything works:

```bash
# Run pre-compilation tests
python -m pytest tests/test_precompiled.py -v

# Run all tests (requires additional dependencies)
python -m pytest tests/ -v
```

## Architecture

### File Structure

```
pybag/
├── src/pybag/
│   ├── ros2/                      # Message definitions
│   │   ├── humble/                # Humble distro
│   │   │   ├── builtin_interfaces.py
│   │   │   ├── std_msgs.py
│   │   │   ├── geometry_msgs.py
│   │   │   └── ...
│   │   └── iron/                  # Iron distro (future)
│   │
│   ├── precompiled/               # Pre-compiled code
│   │   ├── __init__.py           # Distro-agnostic interface
│   │   ├── humble.py             # Humble pre-compiled code
│   │   └── iron.py               # Iron pre-compiled code (future)
│   │
│   ├── schema/                    # Schema parsing and compilation
│   │   ├── compiler.py           # Runtime compilation
│   │   └── ros2msg.py            # .msg file parsing
│   │
│   ├── serialize.py              # Checks for pre-compiled encoders
│   └── deserialize.py            # Checks for pre-compiled decoders
│
├── scripts/
│   ├── generate_messages.py      # Main generation script
│   ├── precompile_messages.py    # Legacy script
│   └── README.md                 # Scripts documentation
│
├── benchmarks/
│   └── precompile_benchmark.py   # Performance benchmark
│
└── tests/
    └── test_precompiled.py        # Pre-compilation tests
```

### Code Flow

#### Build Time (Code Generation)

```
Message Dataclasses → Schema Parsing → Code Generation → Python File
        ↓                    ↓               ↓               ↓
   std_msgs.Header    Schema object    Function code    humble.py
```

#### Runtime (Message Serialization)

```
User Code → Serializer → Check Precompiled → Use Function → Bytes
              ↓              ↓ (if exists)       ↓
           Header      get_encoder()      encode_std_msgs_msg_Header()
              ↓              ↓ (if not)
              └──────→ Runtime Compile → Use Function → Bytes
```

## Performance Benefits

### Benchmark Results

| Message Type | Serialize Speedup | Deserialize Speedup |
|--------------|------------------|---------------------|
| std_msgs/msg/Header | 28.31x | 141.95x |
| geometry_msgs/msg/Point | 33.72x | 184.71x |
| geometry_msgs/msg/Pose | 43.97x | 262.95x |
| geometry_msgs/msg/PoseStamped | 53.20x | 268.80x |

**Average: 39.80x faster serialization, 214.60x faster deserialization**

### Why So Fast?

1. **No `exec()` overhead**: Pre-compiled functions are regular Python code
2. **Optimized operations**: Batch struct packing/unpacking
3. **No code generation delay**: Functions are ready to use
4. **Minimal allocations**: Efficient memory usage

## Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'pybag'"

**Solution**: Install pybag in editable mode
```bash
pip install -e .
```

### Issue: Pre-compiled functions not being used

**Debug Steps**:

1. Check the message name matches exactly:
   ```python
   # Correct
   msg.__msg_name__  # Should be 'std_msgs/msg/Header'
   ```

2. Verify the function exists:
   ```python
   from pybag import precompiled
   func = precompiled.get_decoder('std_msgs/msg/Header')
   print(func)  # Should not be None
   ```

3. Check initialization:
   ```python
   from pybag.precompiled import humble
   humble.initialize_dataclass_types()
   ```

### Issue: Generated code has syntax errors

**Causes**:
- Corrupted message definitions
- Unsupported field types

**Solution**: Regenerate with verbose output
```bash
python scripts/generate_messages.py --precompile --packages <package> 2>&1 | tee generation.log
```

## Best Practices

1. **Version Control**: Commit generated code to git
   - Pre-compiled code is deterministic
   - Easier to track changes
   - No build step required for users

2. **Regenerate After Updates**: When adding/modifying messages
   ```bash
   python scripts/generate_messages.py --precompile
   git diff src/pybag/precompiled/  # Review changes
   ```

3. **Test Before Commit**: Always run tests
   ```bash
   python -m pytest tests/test_precompiled.py
   python benchmarks/precompile_benchmark.py
   ```

4. **Document Custom Packages**: If adding custom messages, document them
   ```python
   # src/pybag/ros2/humble/my_package.py
   """Custom message types for my application.

   Generated from: https://github.com/my-org/my-msgs
   Version: 1.2.3
   """
   ```

## Future Enhancements

Planned features for the generation scripts:

- [ ] **Download .msg files from GitHub**: Automatically fetch message definitions
- [ ] **Incremental generation**: Only regenerate changed messages
- [ ] **Service and Action support**: Generate code for .srv and .action files
- [ ] **Custom template support**: Allow user-defined code templates
- [ ] **Multi-distro auto-detection**: Automatically detect and generate for all distros
- [ ] **Dependency resolution**: Automatically include required packages
- [ ] **Validation**: Check generated code for correctness

## Summary

The message generation workflow:

1. ✅ **Message definitions** exist in `src/pybag/ros2/<distro>/`
2. ✅ **Run generator** with `scripts/generate_messages.py`
3. ✅ **Pre-compiled code** generated in `src/pybag/precompiled/`
4. ✅ **Automatic runtime usage** via `serialize.py` / `deserialize.py`
5. ✅ **Massive performance boost** for standard messages
6. ✅ **Seamless fallback** for custom messages

For more details, see [scripts/README.md](scripts/README.md).
