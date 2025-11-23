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

# Sequential reading
with McapFileReader.from_file("data.mcap") as reader:
    for msg in reader.messages("/camera"):
        print(msg.log_time, msg.data)

# Random access by index (O(1) access for ML training workflows)
with McapFileReader.from_file("data.mcap") as reader:
    # Get total number of messages in a topic
    count = reader.get_message_count("/camera")

    # Access specific message by index
    first_msg = reader.get_message_at_index("/camera", 0)
    last_msg = reader.get_message_at_index("/camera", count - 1)

    # Get batch of messages by index range
    for msg in reader.messages_by_index("/camera", start_index=100, end_index=200):
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
