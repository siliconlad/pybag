# Message Generation Scripts

This directory contains scripts for generating ROS2 message dataclasses and pre-compiled encoders/decoders.

## Scripts

### `generate_messages.py`

A comprehensive tool for generating ROS2 message types and pre-compiled code.

#### Features

1. **Generate Message Dataclasses**: Create Python dataclasses from ROS2 .msg definitions
2. **Generate Pre-compiled Encoders/Decoders**: Create optimized encoder/decoder functions
3. **Multi-distro Support**: Works with different ROS2 distributions (Humble, Iron, Jazzy, etc.)
4. **Selective Package Generation**: Generate code for specific packages only

#### Usage

```bash
# Generate pre-compiled encoders/decoders for Humble (default)
python scripts/generate_messages.py --precompile

# Generate for specific ROS2 distribution
python scripts/generate_messages.py --distro iron --precompile

# Generate for specific packages only
python scripts/generate_messages.py --distro humble --precompile --packages std_msgs geometry_msgs

# Generate dataclasses
python scripts/generate_messages.py --distro humble --dataclasses --output-dir src/pybag/ros2/humble

# Generate both dataclasses and pre-compiled code
python scripts/generate_messages.py --distro humble --dataclasses --precompile

# Custom output locations
python scripts/generate_messages.py \
  --distro humble \
  --precompile \
  --precompiled-output src/pybag/precompiled/humble.py
```

#### Options

- `--distro DISTRO`: ROS2 distribution (default: `humble`)
  - Supported: `humble`, `iron`, `jazzy`, `rolling`

- `--dataclasses`: Generate message dataclasses from existing types

- `--precompile`: Generate pre-compiled encoder/decoder functions

- `--packages PKG [PKG ...]`: Specific packages to process
  - Default: `builtin_interfaces`, `std_msgs`, `geometry_msgs`, `nav_msgs`, `sensor_msgs`

- `--output-dir DIR`: Output directory for dataclasses
  - Default: `src/pybag/ros2/<distro>`

- `--precompiled-output FILE`: Output file for pre-compiled code
  - Default: `src/pybag/precompiled/<distro>.py`

#### Examples

**Regenerate pre-compiled code after updates:**
```bash
python scripts/generate_messages.py --distro humble --precompile
```

**Generate code for additional packages:**
```bash
python scripts/generate_messages.py \
  --distro humble \
  --precompile \
  --packages std_msgs geometry_msgs sensor_msgs tf2_msgs
```

**Support a new ROS2 distribution:**
```bash
# 1. Create directory structure
mkdir -p src/pybag/ros2/jazzy

# 2. Generate dataclasses and pre-compiled code
python scripts/generate_messages.py \
  --distro jazzy \
  --dataclasses \
  --precompile \
  --output-dir src/pybag/ros2/jazzy
```

### `precompile_messages.py`

Legacy script for pre-compiling messages. This script pre-compiles all standard ROS2 messages from the `humble` distribution.

#### Usage

```bash
python scripts/precompile_messages.py
```

This script:
- Loads all message types from `pybag.ros2.humble`
- Generates optimized encoder/decoder functions
- Saves to `src/pybag/precompiled/humble.py`

**Note:** This script is maintained for backward compatibility. Use `generate_messages.py` for new work.

## Performance Benefits

Pre-compilation provides significant performance improvements:

- **28-53x faster** serialization on first use
- **142-269x faster** deserialization on first use
- **Average: 39.80x faster serialization, 214.60x faster deserialization**

## How It Works

### Pre-compilation Process

1. **Schema Parsing**: Message types are analyzed to extract their structure
2. **Code Generation**: Optimized Python code is generated for encoding/decoding
3. **Static Storage**: Generated functions are saved as regular Python code
4. **Runtime Loading**: Functions are loaded directly without `exec()` overhead

### Runtime Behavior

When serializing/deserializing a message:

1. Check if a pre-compiled function exists for the message type
2. If yes: Use the pre-compiled function (fast path)
3. If no: Compile at runtime using `exec()` (fallback for custom messages)

This is completely transparent - no code changes required!

## Adding New Message Types

To add support for new message types:

1. **Add message definitions** to the appropriate package in `src/pybag/ros2/<distro>/`

2. **Regenerate pre-compiled code**:
   ```bash
   python scripts/generate_messages.py --distro humble --precompile
   ```

3. **Test the new messages**:
   ```python
   from pybag.ros2.humble import your_new_package
   from pybag import precompiled

   # Verify pre-compiled functions exist
   decoder = precompiled.get_decoder('your_new_package/msg/YourMessage')
   assert decoder is not None
   ```

## Supporting Multiple ROS2 Distributions

To support multiple ROS2 distributions (e.g., Humble and Iron):

1. **Generate code for each distro**:
   ```bash
   python scripts/generate_messages.py --distro humble --precompile
   python scripts/generate_messages.py --distro iron --precompile
   ```

2. **Update `precompiled/__init__.py`** to select the correct distro at runtime:
   ```python
   def get_decoder(msg_name: str, distro: str = 'humble') -> Callable | None:
       module = importlib.import_module(f'pybag.precompiled.{distro}')
       return module.get_decoder(msg_name)
   ```

## Troubleshooting

### Error: "No module named 'pybag'"

Install pybag in editable mode:
```bash
pip install -e .
```

### Error: "Unknown package 'my_package', cannot auto-download"

The package is not in the standard ROS2 repositories. You'll need to:

1. Manually create the message dataclasses in `src/pybag/ros2/<distro>/my_package.py`
2. Run the pre-compilation script to generate encoder/decoders

### Pre-compiled functions not being used

Check that:
1. The message type name matches exactly (e.g., `std_msgs/msg/Header`)
2. The pre-compiled module was generated successfully
3. The `initialize_dataclass_types()` function is being called

Debug by enabling logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Development

### Running Tests

```bash
# Test pre-compilation
python -m pytest tests/test_precompiled.py -v

# Run benchmarks
python benchmarks/precompile_benchmark.py
```

### Code Quality

The generated code follows these principles:
- No dependencies beyond stdlib and pybag
- Type-annotated for better IDE support
- Optimized for performance (batch struct operations, minimal allocations)
- Human-readable (for debugging and understanding)

## Future Enhancements

Potential improvements:
- [ ] Download .msg files directly from GitHub
- [ ] Support for ROS2 services (.srv) and actions (.action)
- [ ] Incremental compilation (only recompile changed messages)
- [ ] Generate from rosdep package names
- [ ] Support for custom message repositories
