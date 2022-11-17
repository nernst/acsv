#!/usr/bin/env python3
import pathlib
import sys
import unittest

from . import Reader
from .util import AsyncTextFile, aenumerate

assert sys.version_info >= (3, 10)


class ReaderTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        super().setUp()

        self._samples_dir = pathlib.Path(__file__).parent / "../test_files"

    async def test_simple_csv(self):
        expected = (
            ["Column1", "Column2"],
            ["123", "asdf"],
            ["456", "hjkl"],
            ["789", "The quick brown fox jumped over the lazy log, and then continued along."],
            ["012", "This string \" has an embedded quote."],
            ["345", "This row has an extra column", "Extra Value"],
            ["678"],
        )
        async with AsyncTextFile(self._samples_dir / "simple.csv", "r") as fp:
            reader = Reader(fp)
            async for index, row in aenumerate(reader):
                if index < len(expected):
                    self.assertEqual(expected[index], row)
                else:
                    self.assertFalse(f"Only expected {len(expected)} rows. {index=}, {row=}")

            self.assertEqual(expected[0], reader.fieldnames)


if __name__ == "__main__":
    unittest.main()
