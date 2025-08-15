import logging
import re
from collections import defaultdict
from enum import Enum

from pybag.mcap.records import SchemaRecord

logger = logging.getLogger(__name__)

# Map ROS1 types to Python types
PRIMITIVE_TYPE_MAP = {
    "bool": bool,
    "int8": int,
    "uint8": int,
    "int16": int,
    "uint16": int,
    "int32": int,
    "uint32": int,
    "int64": int,
    "uint64": int,
    "float32": float,
    "float64": float,
    "string": str,
    "time": int,
    "duration": int,
    "char": int,
    "byte": int,
}


class Ros1MsgFieldType(str, Enum):
    PRIMITIVE = "primitive"
    ARRAY = "array"
    SEQUENCE = "sequence"
    COMPLEX = "complex"


class Ros1MsgError(Exception):
    """Exception raised for errors in the ROS1 message parsing."""

    def __init__(self, message: str):
        super().__init__(message)


def parse_ros1msg_type(field_raw_type: str, package_name: str) -> dict:
    string_length_match = re.match(r"string(.*)\[", field_raw_type)

    if re.match(r".*\[.*\]$", field_raw_type):
        if match := re.match(r".*\[(\d*)\]$", field_raw_type):
            field_type = Ros1MsgFieldType.ARRAY
            length = int(match.group(1))
        else:
            field_type = Ros1MsgFieldType.SEQUENCE
            length = None

        if string_length_match:
            logger.debug("String is limited: %s", string_length_match.group(1))

        element_type = re.match(r"^(.*)\[", field_raw_type).group(1)
        return {
            "field_type": field_type,
            "data_type": element_type,
            "length": length,
        }

    if "string" in field_raw_type and string_length_match:
        logger.debug("String is limited: %s", string_length_match.group(1))
        return {"field_type": Ros1MsgFieldType.PRIMITIVE, "data_type": "string"}

    if field_raw_type in PRIMITIVE_TYPE_MAP:
        return {"field_type": Ros1MsgFieldType.PRIMITIVE, "data_type": field_raw_type}

    data_type = field_raw_type
    if field_raw_type == "Header":
        data_type = "std_msgs/Header"
    elif "/" not in field_raw_type:
        data_type = f"{package_name}/{field_raw_type}"
    return {"field_type": Ros1MsgFieldType.COMPLEX, "data_type": data_type}


def parse_ros1msg_field(package_name: str, field: str) -> tuple[str, dict]:
    field = re.sub(r"#.*\n", "", field)

    field_raw_type, field_raw_name = field.split()[:2]
    if "=" in field_raw_name:
        raise Ros1MsgError("Constant values are not supported yet")

    field_name = field_raw_name
    field_type = parse_ros1msg_type(field_raw_type, package_name)

    return (
        field_name,
        {
            "field_type": field_type["field_type"],
            "data_type": field_type["data_type"],
            "length": field_type.get("length"),
        },
    )


def parse_ros1msg(schema: SchemaRecord) -> tuple[dict, dict]:
    """Parse a ros1msg schema record."""
    assert schema.encoding == "ros1msg"
    logger.debug("Parsing schema: %s", schema.name)

    package_name = schema.name.split("/")[0]
    msg = schema.data.decode("utf-8")

    msg = "\n".join([line.strip() for line in msg.split("\n")])
    msg = re.sub(r"#.*\n", "", msg, flags=re.MULTILINE)
    msg = re.sub(r"\n\n", "\n", msg, flags=re.MULTILINE)

    msg = [m.strip() for m in msg.split("=" * 80)]

    msg_schema = defaultdict(dict)
    main_fields = [m.strip() for m in msg[0].split("\n") if m]
    for field in main_fields:
        field_name, field_dict = parse_ros1msg_field(package_name, field)
        msg_schema[field_name] = field_dict

    schema_msgs = defaultdict(dict)
    for sub_msg in msg[1:]:
        sub_msg_name = sub_msg.split("\n")[0].strip()[5:]
        sub_msg_fields = [m.strip() for m in sub_msg.split("\n")[1:] if m]
        for field in sub_msg_fields:
            field_name, field_dict = parse_ros1msg_field(package_name, field)
            schema_msgs[sub_msg_name][field_name] = field_dict

    return msg_schema, schema_msgs

