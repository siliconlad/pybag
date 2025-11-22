# pybag

A Python library to work with bag files (MCAP format) without ROS.

> [!Warning]
> The library is still in the early stages of development and the API is still unstable

## Installation

```bash
uv add pybag-sdk
```

## Quick Start

```bash
# Get file information
pybag info data.mcap

# Filter messages
pybag filter data.mcap -o output.mcap --include-topic /camera

# Merge multiple files
pybag merge input1.mcap input2.mcap -o output.mcap
```

### Reading MCAP Files

```python
from pybag.mcap_reader import McapFileReader

with McapFileReader.from_file("data.mcap") as reader:
    for msg in reader.messages("/camera"):
        print(msg.log_time, msg.data)
```

### Writing MCAP Files

```python
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import std_msgs

with McapFileWriter.open("output.mcap") as writer:
    log_time_ns = 1000
    msg = std_msgs.String(data="hello")
    writer.write_message("/status", log_time_ns, msg)
```

### Custom Messages

```python
from dataclasses import dataclass
import pybag

@dataclass
class SensorData:
    __msg_name__ = 'custom/msg/SensorData'
    temperature: pybag.float32
    humidity: pybag.float32

with McapFileWriter.open("sensors.mcap") as writer:
    writer.write_message("/sensor", 1000, SensorData(25.5, 60.0))
```

## Performance Optimizations

### Pre-compiled Message Types

pybag includes pre-compiled encoder/decoder functions for standard ROS2 message types, providing significant performance improvements for first-time message serialization/deserialization:

- **28-53x faster** serialization on first use
- **142-269x faster** deserialization on first use

Pre-compiled message types include:
- `builtin_interfaces` (Time, Duration)
- `std_msgs` (Header, String, Int32, Float64, etc.)
- `geometry_msgs` (Point, Pose, Transform, Twist, etc.)
- `nav_msgs` (Odometry, Path, OccupancyGrid, etc.)
- `sensor_msgs` (Image, LaserScan, PointCloud2, Imu, etc.)

The pre-compilation is automatic and transparent - no code changes required. Custom message types automatically fall back to runtime compilation.

#### Re-generating Pre-compiled Messages

If you need to regenerate the pre-compiled message types (e.g., after adding new standard messages):

```bash
python scripts/generate_messages.py --distro humble --precompile
```

For more advanced options (different distros, specific packages, generating dataclasses), see [scripts/README.md](scripts/README.md).
