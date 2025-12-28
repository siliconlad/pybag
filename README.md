# pybag

A Python library to work with bag files (MCAP and ROS Bag) without ROS.

> [!Warning]
> The library is still in the early stages of development and the API is still unstable

## Installation

```bash
# Add dependency to your package
uv add pybag-sdk

# Install pybag cli
uv tool install pybag-sdk
```

## Quick Start

### CLI

```bash
# Get file information
pybag info data.mcap

# Filter messages
pybag filter data.mcap -o output.mcap --include-topic /camera

# Merge multiple files
pybag merge input1.mcap input2.mcap -o output.mcap
```

### Unified Reader

The `Reader` class provides a common interface for reading both MCAP and ROS 1 bag files.

The file format is automatically detected from the file extension.

```python
from pybag import Reader

# Works with both .mcap and .bag files
with Reader.from_file("data.mcap") as reader:
    for msg in reader.messages("/camera"):
        print(msg.log_time, msg.data)

# Same API for ROS 1 bag files
with Reader.from_file("data.bag") as reader:
    for msg in reader.messages("/sensor/*"):  # glob patterns supported
        print(msg.topic, msg.msg_type, msg.data)
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

### Reading ROS 1 Bag Files

```python
from pybag.bag_reader import BagFileReader

with BagFileReader.from_file("data.bag") as reader:
    for msg in reader.messages("/camera"):
        print(msg.log_time, msg.data)
```

### Writing ROS 1 Bag Files

```python
from pybag.bag_writer import BagFileWriter
from pybag.ros1.noetic import std_msgs

with BagFileWriter.open("output.bag") as writer:
    log_time_ns = 1000
    msg = std_msgs.String(data="hello")
    writer.write_message("/status", log_time_ns, msg)
```

### Custom Messages

```python
from dataclasses import dataclass
import pybag.types as t

@dataclass
class SensorData:
    __msg_name__ = 'custom/msg/SensorData'
    temperature: t.float32
    humidity: t.float32

with McapFileWriter.open("sensors.mcap") as writer:
    writer.write_message("/sensor", 1000, SensorData(25.5, 60.0))
```
