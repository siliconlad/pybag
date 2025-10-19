import pytest

from pybag.encoding.ros1 import Ros1Decoder, Ros1Encoder


@pytest.mark.parametrize("little_endian", [True, False])
def test_encode_decode_all_primitive_types(little_endian: bool) -> None:
    data_values = [
        ("bool", [True, False]),
        ("int8", [-8, 8]),
        ("uint8", [0, 200]),
        ("int16", [-12_345, 12_345]),
        ("uint16", [0, 54_321]),
        ("int32", [-12_345_678, 12_345_678]),
        ("uint32", [0, 12_345_678]),
        ("int64", [-12_345_678_901, 12_345_678_901]),
        ("uint64", [0, 9_876_543_210]),
        ("float32", [-0.5, 0.5]),
        ("float64", [0.0009765625, -0.0009765625]),
        ("string", ["", "hello world"]),
    ]

    encoder = Ros1Encoder(little_endian=little_endian)
    for type_name, values in data_values:
        for v in values:
            encoder.encode(type_name, v)

    data = encoder.save()

    decoder = Ros1Decoder(data, little_endian=little_endian)
    for type_name, values in data_values:
        for v in values:
            assert decoder.parse(type_name) == v


@pytest.mark.parametrize("little_endian", [True, False])
def test_encode_decode_array(little_endian: bool) -> None:
    encoder = Ros1Encoder(little_endian=little_endian)
    encoder.array("int32", [1, 2, 3])

    decoder = Ros1Decoder(encoder.save(), little_endian=little_endian)
    assert decoder.array("int32", 3) == [1, 2, 3]


@pytest.mark.parametrize("little_endian", [True, False])
def test_encode_decode_sequence(little_endian: bool) -> None:
    encoder = Ros1Encoder(little_endian=little_endian)
    encoder.sequence("int32", [1, 2, 3])

    decoder = Ros1Decoder(encoder.save(), little_endian=little_endian)
    assert decoder.sequence("int32") == [1, 2, 3]
