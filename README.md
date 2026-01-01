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

## Reading Files

There are a number of ways to read data with pybag.

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
    # Get available topics
    topics = reader.get_topics()

    # Stream messages with filtering
    for msg in reader.messages(
        topic=["/camera/*", "/sensor/imu"],
        start_time=1_000_000_000,  # nanoseconds
        end_time=2_000_000_000,
        in_log_time_order=True
    ):
        print(msg.log_time, msg.data)

    # Get attachments and metadata
    attachments = reader.get_attachments()
    metadata = reader.get_metadata()
```

### Reading ROS 1 Bag Files

```python
from pybag.bag_reader import BagFileReader

with BagFileReader.from_file("data.bag") as reader:
    for msg in reader.messages("/camera"):
        print(msg.log_time, msg.data)
```

## Writing Files

There are a number of ways to write data with pybag.

### Writing MCAP Files

```python
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import std_msgs

with McapFileWriter.open(
    "output.mcap",
    profile="ros2",
    chunk_compression="lz4"
) as writer:
    msg = std_msgs.String(data="hello")
    writer.write_message("/status", 1_000_000_000, msg)

    # Add attachments
    writer.write_attachment(
        name="calibration.yaml",
        data=b"camera: ...",
        media_type="application/yaml"
    )

    # Add metadata
    writer.write_metadata("recording_info", {"robot": "husky_1"})
```

### Writing ROS 1 Bag Files

```python
from pybag.bag_writer import BagFileWriter
from pybag.ros1.noetic import std_msgs

with BagFileWriter.open("output.bag", compression="bz2") as writer:
    msg = std_msgs.String(data="hello")
    writer.write_message("/status", 1_000_000_000, msg)
```

## Append Mode

Both MCAP and bag writers support append mode to add messages to existing files.

### Appending to MCAP Files

```python
from pybag.mcap_writer import McapFileWriter

# Create a new file
with McapFileWriter.open("recording.mcap") as writer:
    writer.write_message("/topic", 1_000_000_000, msg1)

# Append to existing file
with McapFileWriter.open("recording.mcap", mode="a") as writer:
    writer.write_message("/topic", 2_000_000_000, msg2)
    writer.write_message("/new_topic", 3_000_000_000, msg3)
```

### Appending to ROS 1 Bag Files

```python
from pybag.bag_writer import BagFileWriter

# Create a new file
with BagFileWriter.open("recording.bag") as writer:
    writer.write_message("/topic", 1_000_000_000, msg1)

# Append to existing file
with BagFileWriter.open("recording.bag", mode="a") as writer:
    writer.write_message("/topic", 2_000_000_000, msg2)
    writer.write_message("/new_topic", 3_000_000_000, msg3)
```

## TypeStore

The `TypeStore` provides unified access to ROS message schemas from built-in definitions
and custom `.msg` files.

```python
from pybag.typestore import TypeStore

# Create a type store for ROS 2 Humble
type_store = TypeStore(encoding="ros2msg", distro="humble")

# Add custom message paths
type_store.add_path("/path/to/custom_msgs")

# Find a message schema
schema = type_store.find("std_msgs/msg/String")

# List user-provided messages
messages = type_store.list_messages()
```

### Supported Distributions

**ROS 1**: noetic (and legacy: melodic, kinetic, indigo, etc.)

**ROS 2**: foxy, galactic, humble, iron, jazzy, kilted, rolling

## Custom Messages

Define custom message types using dataclasses:

```python
from dataclasses import dataclass
import pybag.types as t
from pybag.mcap_writer import McapFileWriter

@dataclass
class SensorData:
    __msg_name__ = "custom/msg/SensorData"
    temperature: t.float32
    humidity: t.float32

with McapFileWriter.open("sensors.mcap") as writer:
    writer.write_message("/sensor", 1_000_000_000, SensorData(25.5, 60.0))
```

Or provide schema text directly:

```python
from pybag.types import SchemaText

schema = SchemaText(
    name="custom/msg/SensorData",
    text="float32 temperature\nfloat32 humidity"
)

with McapFileWriter.open("sensors.mcap") as writer:
    writer.add_channel("/sensor", schema=schema)
    writer.write_message("/sensor", 1_000_000_000, {"temperature": 25.5, "humidity": 60.0})
```

## CLI Reference

### info

Display file statistics and metadata:

```bash
pybag info recording.mcap
pybag info recording.bag
```

### filter

Extract data based on topic patterns and time ranges:

```bash
pybag filter input.mcap -o output.mcap \
    --include-topic "/sensor/*" \
    --exclude-topic "/sensor/debug" \
    --start-time 10.5 \
    --end-time 20.3
```

### merge

Combine multiple files into one:

```bash
pybag merge file1.mcap file2.mcap -o merged.mcap
pybag merge bag1.bag bag2.bag -o merged.bag
```

### convert

Convert between bag and MCAP formats:

```bash
# Bag to MCAP
pybag convert input.bag -o output.mcap --profile ros2 --mcap-compression lz4

# MCAP to Bag
pybag convert input.mcap -o output.bag --bag-compression bz2
```

### sort

Sort messages by time and/or topic:

```bash
pybag sort input.mcap -o sorted.mcap --log-time --by-topic
```

### recover

Recover data from corrupted files:

```bash
pybag recover corrupted.mcap -o recovered.mcap --verbose
```

### inspect

Examine specific record types:

```bash
pybag inspect schemas recording.mcap
pybag inspect channels recording.mcap --topic "/camera/*"
pybag inspect metadata recording.mcap --name "calibration"
pybag inspect attachments recording.mcap --name "calib.yaml" --data
```
