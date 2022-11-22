#!/usr/bin/env python3
import pathlib
import sys
import unittest

from acsv import DictReader
from acsv.util import AsyncTextFile, aenumerate

assert sys.version_info >= (3, 10)


class DictReaderTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        super().setUp()

        self._samples_dir = pathlib.Path(__file__).parent / "../test_files"

    async def test_simple_csv(self):
        header = ["Column1", "Column2"]
        expected = (
            ["123", "asdf"],
            ["456", "hjkl"],
            ["789", "The quick brown fox jumped over the lazy log, and then continued along."],
            ["012", "This string \" has an embedded quote."],
            ["345", "This row has an extra column", "Extra Value"],
            ["678", None],
        )
        async with AsyncTextFile(self._samples_dir / "simple.csv", "r") as fp:
            reader = DictReader(fp)
            async for index, row in aenumerate(reader):
                if index < len(expected):
                    expected_row = expected[index]
                    d = dict(zip(header, expected_row[:len(header)]))

                    if len(expected_row) > len(header):
                        d[None] = list(expected_row[len(header):])
                    self.assertEqual(d, row)
                else:
                    self.assertFalse(f"Only expected {len(expected)} rows. {index=}, {row=}")

            self.assertEqual(header, reader.fieldnames)


if __name__ == "__main__":
    unittest.main()

