"""Tests for ROS 1 message serialization (rosmsg format)."""

import struct

import pytest

from pybag.encoding.rosmsg import RosmsgDecoder, RosmsgEncoder


class TestRosmsgEncoder:
    """Tests for the RosmsgEncoder class."""

    def test_encode_bool(self):
        encoder = RosmsgEncoder()
        encoder.bool(True)
        encoder.bool(False)
        data = encoder.save()
        assert data == b'\x01\x00'

    def test_encode_int8(self):
        encoder = RosmsgEncoder()
        encoder.int8(-128)
        encoder.int8(127)
        data = encoder.save()
        assert data == struct.pack('<bb', -128, 127)

    def test_encode_uint8(self):
        encoder = RosmsgEncoder()
        encoder.uint8(0)
        encoder.uint8(255)
        data = encoder.save()
        assert data == struct.pack('<BB', 0, 255)

    def test_encode_int32(self):
        encoder = RosmsgEncoder()
        encoder.int32(-2147483648)
        encoder.int32(2147483647)
        data = encoder.save()
        assert data == struct.pack('<ii', -2147483648, 2147483647)

    def test_encode_float64(self):
        encoder = RosmsgEncoder()
        encoder.float64(3.14159265359)
        data = encoder.save()
        assert data == struct.pack('<d', 3.14159265359)

    def test_encode_string_no_null_terminator(self):
        """Verify that rosmsg strings do NOT have null terminators."""
        encoder = RosmsgEncoder()
        encoder.string("hello")
        data = encoder.save()
        # Format: 4-byte length + string bytes (no null terminator)
        assert data == struct.pack('<I', 5) + b'hello'
        assert len(data) == 4 + 5  # length prefix + string, no null

    def test_encode_empty_string(self):
        encoder = RosmsgEncoder()
        encoder.string("")
        data = encoder.save()
        assert data == struct.pack('<I', 0)  # Just the length = 0

    def test_encode_sequence(self):
        encoder = RosmsgEncoder()
        encoder.sequence('int32', [1, 2, 3])
        data = encoder.save()
        # 4-byte count + 3 * 4-byte ints
        expected = struct.pack('<I', 3) + struct.pack('<iii', 1, 2, 3)
        assert data == expected

    def test_encode_array(self):
        encoder = RosmsgEncoder()
        encoder.array('int32', [10, 20, 30])
        data = encoder.save()
        # Fixed array: just the values, no length prefix
        expected = struct.pack('<iii', 10, 20, 30)
        assert data == expected

    def test_no_alignment_padding(self):
        """Verify that rosmsg does NOT add alignment padding."""
        encoder = RosmsgEncoder()
        encoder.uint8(1)
        encoder.int32(42)  # Would need padding in CDR
        data = encoder.save()
        # Should be 1 + 4 = 5 bytes, no padding
        assert len(data) == 5
        assert data == struct.pack('<B', 1) + struct.pack('<i', 42)


class TestRosmsgDecoder:
    """Tests for the RosmsgDecoder class."""

    def test_decode_bool(self):
        data = b'\x01\x00'
        decoder = RosmsgDecoder(data)
        assert decoder.bool() is True
        assert decoder.bool() is False

    def test_decode_int8(self):
        data = struct.pack('<bb', -128, 127)
        decoder = RosmsgDecoder(data)
        assert decoder.int8() == -128
        assert decoder.int8() == 127

    def test_decode_uint8(self):
        data = struct.pack('<BB', 0, 255)
        decoder = RosmsgDecoder(data)
        assert decoder.uint8() == 0
        assert decoder.uint8() == 255

    def test_decode_int32(self):
        data = struct.pack('<ii', -2147483648, 2147483647)
        decoder = RosmsgDecoder(data)
        assert decoder.int32() == -2147483648
        assert decoder.int32() == 2147483647

    def test_decode_float64(self):
        data = struct.pack('<d', 3.14159265359)
        decoder = RosmsgDecoder(data)
        assert decoder.float64() == pytest.approx(3.14159265359)

    def test_decode_string_no_null_terminator(self):
        """Verify that rosmsg strings do NOT expect null terminators."""
        # 4-byte length + string bytes (no null)
        data = struct.pack('<I', 5) + b'hello'
        decoder = RosmsgDecoder(data)
        assert decoder.string() == "hello"

    def test_decode_empty_string(self):
        data = struct.pack('<I', 0)
        decoder = RosmsgDecoder(data)
        assert decoder.string() == ""

    def test_decode_sequence(self):
        data = struct.pack('<I', 3) + struct.pack('<iii', 1, 2, 3)
        decoder = RosmsgDecoder(data)
        assert decoder.sequence('int32') == [1, 2, 3]

    def test_decode_array(self):
        data = struct.pack('<iii', 10, 20, 30)
        decoder = RosmsgDecoder(data)
        assert decoder.array('int32', 3) == [10, 20, 30]


class TestRosmsgRoundtrip:
    """Test encoding and decoding roundtrips."""

    def test_roundtrip_primitives(self):
        encoder = RosmsgEncoder()
        encoder.bool(True)
        encoder.int8(-42)
        encoder.uint32(12345)
        encoder.float64(2.71828)
        encoder.string("test message")

        data = encoder.save()
        decoder = RosmsgDecoder(data)

        assert decoder.bool() is True
        assert decoder.int8() == -42
        assert decoder.uint32() == 12345
        assert decoder.float64() == pytest.approx(2.71828)
        assert decoder.string() == "test message"

    def test_roundtrip_mixed_types(self):
        """Test that no padding is added between mixed types."""
        encoder = RosmsgEncoder()
        encoder.uint8(1)
        encoder.string("hi")
        encoder.int32(100)
        encoder.sequence('uint8', [10, 20, 30])

        data = encoder.save()
        decoder = RosmsgDecoder(data)

        assert decoder.uint8() == 1
        assert decoder.string() == "hi"
        assert decoder.int32() == 100
        assert decoder.sequence('uint8') == [10, 20, 30]
