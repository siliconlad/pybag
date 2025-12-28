"""Tests for ROS 1 message serialization (rosmsg format)."""

import struct

import pytest

from pybag.encoding.rosmsg import RosMsgDecoder, RosMsgEncoder
from pybag.types import ros1


class TestRosmsgRoundtrip:
    """Test encoding and decoding roundtrips."""

    def test_roundtrip_char(self):
        encoder = RosMsgEncoder()
        encoder.char(0)
        encoder.char(255)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.char() == 0
        assert decoder.char() == 255

    def test_roundtrip_time(self):
        encoder = RosMsgEncoder()
        encoder.time(ros1.Time(secs=0, nsecs=0))
        encoder.time(ros1.Time(secs=1234567890, nsecs=123456789))
        encoder.time(ros1.Time(secs=4294967295, nsecs=999999999))  # max uint32 sec

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.time() == ros1.Time(secs=0, nsecs=0)
        assert decoder.time() == ros1.Time(secs=1234567890, nsecs=123456789)
        assert decoder.time() == ros1.Time(secs=4294967295, nsecs=999999999)

    def test_roundtrip_duration(self):
        encoder = RosMsgEncoder()
        encoder.duration(ros1.Duration(secs=0, nsecs=0))
        encoder.duration(ros1.Duration(secs=100, nsecs=500000000))
        encoder.duration(ros1.Duration(secs=3600, nsecs=0))  # 1 hour
        encoder.duration(ros1.Duration(secs=-5, nsecs=-3))  # negative

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.duration() == ros1.Duration(secs=0, nsecs=0)
        assert decoder.duration() == ros1.Duration(secs=100, nsecs=500000000)
        assert decoder.duration() == ros1.Duration(secs=3600, nsecs=0)
        assert decoder.duration() == ros1.Duration(secs=-5, nsecs=-3)

    def test_roundtrip_bool(self):
        encoder = RosMsgEncoder()
        encoder.bool(True)
        encoder.bool(False)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.bool() is True
        assert decoder.bool() is False

    def test_roundtrip_int8(self):
        encoder = RosMsgEncoder()
        encoder.int8(-128)
        encoder.int8(0)
        encoder.int8(127)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.int8() == -128
        assert decoder.int8() == 0
        assert decoder.int8() == 127

    def test_roundtrip_uint8(self):
        encoder = RosMsgEncoder()
        encoder.uint8(0)
        encoder.uint8(128)
        encoder.uint8(255)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.uint8() == 0
        assert decoder.uint8() == 128
        assert decoder.uint8() == 255

    def test_roundtrip_byte(self):
        encoder = RosMsgEncoder()
        encoder.byte(b'\x00')
        encoder.byte(b'\xff')
        encoder.byte(b'\x42')

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.byte() == b'\x00'
        assert decoder.byte() == b'\xff'
        assert decoder.byte() == b'\x42'

    def test_roundtrip_int16(self):
        encoder = RosMsgEncoder()
        encoder.int16(-32768)
        encoder.int16(0)
        encoder.int16(32767)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.int16() == -32768
        assert decoder.int16() == 0
        assert decoder.int16() == 32767

    def test_roundtrip_uint16(self):
        encoder = RosMsgEncoder()
        encoder.uint16(0)
        encoder.uint16(32768)
        encoder.uint16(65535)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.uint16() == 0
        assert decoder.uint16() == 32768
        assert decoder.uint16() == 65535

    def test_roundtrip_int32(self):
        encoder = RosMsgEncoder()
        encoder.int32(-2147483648)
        encoder.int32(0)
        encoder.int32(2147483647)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.int32() == -2147483648
        assert decoder.int32() == 0
        assert decoder.int32() == 2147483647

    def test_roundtrip_uint32(self):
        encoder = RosMsgEncoder()
        encoder.uint32(0)
        encoder.uint32(2147483648)
        encoder.uint32(4294967295)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.uint32() == 0
        assert decoder.uint32() == 2147483648
        assert decoder.uint32() == 4294967295

    def test_roundtrip_int64(self):
        encoder = RosMsgEncoder()
        encoder.int64(-9223372036854775808)
        encoder.int64(0)
        encoder.int64(9223372036854775807)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.int64() == -9223372036854775808
        assert decoder.int64() == 0
        assert decoder.int64() == 9223372036854775807

    def test_roundtrip_uint64(self):
        encoder = RosMsgEncoder()
        encoder.uint64(0)
        encoder.uint64(9223372036854775808)
        encoder.uint64(18446744073709551615)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.uint64() == 0
        assert decoder.uint64() == 9223372036854775808
        assert decoder.uint64() == 18446744073709551615

    def test_roundtrip_float32(self):
        encoder = RosMsgEncoder()
        encoder.float32(0.0)
        encoder.float32(3.14159)
        encoder.float32(-1.5e10)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.float32() == pytest.approx(0.0)
        assert decoder.float32() == pytest.approx(3.14159, rel=1e-5)
        assert decoder.float32() == pytest.approx(-1.5e10, rel=1e-5)

    def test_roundtrip_float64(self):
        encoder = RosMsgEncoder()
        encoder.float64(0.0)
        encoder.float64(3.141592653589793)
        encoder.float64(-1.5e100)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.float64() == pytest.approx(0.0)
        assert decoder.float64() == pytest.approx(3.141592653589793)
        assert decoder.float64() == pytest.approx(-1.5e100)

    def test_roundtrip_string(self):
        encoder = RosMsgEncoder()
        encoder.string("")
        encoder.string("hello")
        encoder.string("unicode: \u00e9\u00e0\u00fc")

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.string() == ""
        assert decoder.string() == "hello"
        assert decoder.string() == "unicode: \u00e9\u00e0\u00fc"

    def test_roundtrip_array(self):
        encoder = RosMsgEncoder()
        encoder.array('int32', [1, 2, 3, 4, 5])
        encoder.array('float64', [1.1, 2.2, 3.3])
        encoder.array('uint8', [0, 128, 255])

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.array('int32', 5) == [1, 2, 3, 4, 5]
        assert decoder.array('float64', 3) == pytest.approx([1.1, 2.2, 3.3])
        assert decoder.array('uint8', 3) == [0, 128, 255]

    def test_roundtrip_sequence(self):
        encoder = RosMsgEncoder()
        encoder.sequence('int32', [10, 20, 30])
        encoder.sequence('string', ["foo", "bar", "baz"])
        encoder.sequence('float32', [])

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.sequence('int32') == [10, 20, 30]
        assert decoder.sequence('string') == ["foo", "bar", "baz"]
        assert decoder.sequence('float32') == []

    def test_roundtrip_nested_containers(self):
        encoder = RosMsgEncoder()
        # Sequence of sequences (as separate calls)
        encoder.uint32(2)  # outer sequence length
        encoder.sequence('int32', [1, 2])
        encoder.sequence('int32', [3, 4, 5])

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        outer_len = decoder.uint32()
        assert outer_len == 2
        assert decoder.sequence('int32') == [1, 2]
        assert decoder.sequence('int32') == [3, 4, 5]

    def test_roundtrip_mixed_types(self):
        encoder = RosMsgEncoder()
        encoder.uint32(42)
        encoder.string("test")
        encoder.float64(3.14)
        encoder.sequence('uint8', [1, 2, 3])
        encoder.bool(True)
        encoder.int64(-1000000000000)

        data = encoder.save()
        decoder = RosMsgDecoder(data)

        assert decoder.uint32() == 42
        assert decoder.string() == "test"
        assert decoder.float64() == pytest.approx(3.14)
        assert decoder.sequence('uint8') == [1, 2, 3]
        assert decoder.bool() is True
        assert decoder.int64() == -1000000000000
