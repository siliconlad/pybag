import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pybag.encoding.cdr import CdrEncoder, CdrDecoder


class TestCdrEncoderDecoder(unittest.TestCase):
    def test_encode_decode_all_types(self) -> None:
        values = [
            ("bool", True),
            ("int8", -8),
            ("uint8", 200),
            ("int16", -12345),
            ("uint16", 54321),
            ("int32", -12345678),
            ("uint32", 12345678),
            ("int64", -1234567890123456789),
            ("uint64", 9876543210987654321),
            ("float32", 3.1415926),
            ("float64", 2.718281828459045),
            ("string", "hello world"),
        ]

        encoder = CdrEncoder(little_endian=True)
        for type_name, value in values:
            encoder.encode(type_name, value)

        data = encoder.save()

        decoder = CdrDecoder(data)

        for type_name, value in values:
            decoded = decoder.parse(type_name)
            if isinstance(value, float):
                places = 6 if type_name == "float32" else 15
                self.assertAlmostEqual(decoded, value, places=places)
            else:
                self.assertEqual(decoded, value)


if __name__ == "__main__":
    unittest.main()
