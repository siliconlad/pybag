"""Tests for JSON encoding and decoding."""

import json

import pytest

from pybag.encoding.json import JsonDecoder, JsonEncoder


def test_encoder_encoding_name() -> None:
    """Test that JsonEncoder returns correct encoding name."""
    assert JsonEncoder.encoding() == "json"


def test_encode_decode_all_primitive_types() -> None:
    """Test encoding and decoding of all primitive types."""
    data_values = [
        ('bool', [True, False]),
        ('int8', [-8, 8]),
        ('uint8', [0, 200]),
        ('int16', [-12_345, 12_345]),
        ('uint16', [0, 54_321]),
        ('int32', [-12_345_678, 12_345_678]),
        ('uint32', [0, 12_345_678]),
        ('int64', [-12_345_678_901, 12_345_678_901]),
        ('uint64', [0, 9_876_543_210]),
        ('float32', [-0.5, 0.5]),
        ('float64', [0.0009765625, -0.0009765625]),
        ('string', ['', 'hello world']),
    ]

    # Encode the data
    encoder = JsonEncoder()
    for type_name, values in data_values:
        for v in values:
            encoder.encode(type_name, v)

    data = encoder.save()

    # Verify it's valid JSON
    parsed = json.loads(data.decode('utf-8'))
    assert isinstance(parsed, list)

    # Decode the data
    decoder = JsonDecoder(data)
    for type_name, values in data_values:
        for v in values:
            decoded = decoder.parse(type_name)
            if type_name in ('float32', 'float64'):
                assert abs(decoded - v) < 1e-9
            else:
                assert decoded == v


def test_encode_decode_bool() -> None:
    """Test boolean encoding/decoding."""
    encoder = JsonEncoder()
    encoder.bool(True)
    encoder.bool(False)

    decoder = JsonDecoder(encoder.save())
    assert decoder.bool() is True
    assert decoder.bool() is False


def test_encode_decode_integers() -> None:
    """Test integer encoding/decoding."""
    encoder = JsonEncoder()
    encoder.int8(-128)
    encoder.uint8(255)
    encoder.int16(-32768)
    encoder.uint16(65535)
    encoder.int32(-2147483648)
    encoder.uint32(4294967295)
    encoder.int64(-9223372036854775808)
    encoder.uint64(18446744073709551615)

    decoder = JsonDecoder(encoder.save())
    assert decoder.int8() == -128
    assert decoder.uint8() == 255
    assert decoder.int16() == -32768
    assert decoder.uint16() == 65535
    assert decoder.int32() == -2147483648
    assert decoder.uint32() == 4294967295
    assert decoder.int64() == -9223372036854775808
    assert decoder.uint64() == 18446744073709551615


def test_encode_decode_floats() -> None:
    """Test float encoding/decoding."""
    encoder = JsonEncoder()
    encoder.float32(3.14)
    encoder.float64(2.718281828459045)

    decoder = JsonDecoder(encoder.save())
    assert abs(decoder.float32() - 3.14) < 1e-6
    assert abs(decoder.float64() - 2.718281828459045) < 1e-15


def test_encode_decode_string() -> None:
    """Test string encoding/decoding."""
    encoder = JsonEncoder()
    encoder.string("")
    encoder.string("hello")
    encoder.string("hello world with spaces")
    encoder.string("unicode: \u00e9\u00e8\u00ea")

    decoder = JsonDecoder(encoder.save())
    assert decoder.string() == ""
    assert decoder.string() == "hello"
    assert decoder.string() == "hello world with spaces"
    assert decoder.string() == "unicode: \u00e9\u00e8\u00ea"


def test_encode_decode_byte() -> None:
    """Test byte encoding/decoding."""
    encoder = JsonEncoder()
    encoder.byte(b'\x00')
    encoder.byte(b'\xff')
    encoder.byte(42)  # Can also encode as int

    decoder = JsonDecoder(encoder.save())
    assert decoder.byte() == b'\x00'
    assert decoder.byte() == b'\xff'
    assert decoder.byte() == b'*'  # 42 as bytes


def test_encode_decode_char() -> None:
    """Test char encoding/decoding."""
    encoder = JsonEncoder()
    encoder.char('A')
    encoder.char('z')

    decoder = JsonDecoder(encoder.save())
    assert decoder.char() == 'A'
    assert decoder.char() == 'z'


def test_encode_decode_array() -> None:
    """Test fixed-size array encoding/decoding."""
    encoder = JsonEncoder()
    encoder.array('int32', [1, 2, 3, 4, 5])

    decoder = JsonDecoder(encoder.save())
    assert decoder.array('int32', 5) == [1, 2, 3, 4, 5]


def test_encode_decode_sequence() -> None:
    """Test variable-size sequence encoding/decoding."""
    encoder = JsonEncoder()
    encoder.sequence('int32', [10, 20, 30])

    decoder = JsonDecoder(encoder.save())
    assert decoder.sequence('int32') == [10, 20, 30]


def test_encode_decode_empty_array() -> None:
    """Test empty array encoding/decoding."""
    encoder = JsonEncoder()
    encoder.array('float64', [])

    decoder = JsonDecoder(encoder.save())
    assert decoder.array('float64', 0) == []


def test_encode_decode_empty_sequence() -> None:
    """Test empty sequence encoding/decoding."""
    encoder = JsonEncoder()
    encoder.sequence('string', [])

    decoder = JsonDecoder(encoder.save())
    assert decoder.sequence('string') == []


def test_encode_decode_string_array() -> None:
    """Test string array encoding/decoding."""
    encoder = JsonEncoder()
    encoder.array('string', ['hello', 'world', '!'])

    decoder = JsonDecoder(encoder.save())
    assert decoder.array('string', 3) == ['hello', 'world', '!']


def test_encode_decode_mixed_types() -> None:
    """Test encoding/decoding of mixed types in sequence."""
    encoder = JsonEncoder()
    encoder.int32(42)
    encoder.string("test")
    encoder.float64(3.14)
    encoder.bool(True)
    encoder.array('uint8', [1, 2, 3])

    decoder = JsonDecoder(encoder.save())
    assert decoder.int32() == 42
    assert decoder.string() == "test"
    assert abs(decoder.float64() - 3.14) < 1e-10
    assert decoder.bool() is True
    assert decoder.array('uint8', 3) == [1, 2, 3]


def test_json_output_is_valid_json() -> None:
    """Test that encoder output is valid JSON."""
    encoder = JsonEncoder()
    encoder.int32(42)
    encoder.string("hello")
    encoder.array('float64', [1.0, 2.0, 3.0])

    data = encoder.save()
    parsed = json.loads(data.decode('utf-8'))

    assert parsed == [42, "hello", [1.0, 2.0, 3.0]]


def test_little_endian_parameter_ignored() -> None:
    """Test that little_endian parameter is accepted but doesn't affect output."""
    encoder_le = JsonEncoder(little_endian=True)
    encoder_be = JsonEncoder(little_endian=False)

    encoder_le.int32(12345)
    encoder_be.int32(12345)

    # Both should produce the same JSON output
    assert encoder_le.save() == encoder_be.save()


def test_decoder_handles_json_from_external_source() -> None:
    """Test that decoder can handle JSON created externally."""
    external_json = json.dumps([42, "hello", 3.14, True]).encode('utf-8')

    decoder = JsonDecoder(external_json)
    assert decoder.int32() == 42
    assert decoder.string() == "hello"
    assert abs(decoder.float64() - 3.14) < 1e-10
    assert decoder.bool() is True


def test_encode_via_encode_method() -> None:
    """Test encoding via the generic encode method."""
    encoder = JsonEncoder()
    encoder.encode('int32', 100)
    encoder.encode('string', 'test')
    encoder.encode('float64', 2.5)

    decoder = JsonDecoder(encoder.save())
    assert decoder.parse('int32') == 100
    assert decoder.parse('string') == 'test'
    assert decoder.parse('float64') == 2.5
