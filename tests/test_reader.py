#!/usr/bin/env python3
import pathlib
import sys
import unittest

from acsv import CsvError, Reader
from acsv.util import aenumerate, AsyncStringIO, AsyncTextFile

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

    async def test_bad_escape(self):
        test = """
Column1,Column2
123,"This column has a bad " quote escape "
""".strip()
        
        async with AsyncStringIO(test, newline='') as fp:
            reader = Reader(fp)
            iter = type(reader).__aiter__(reader)
            anext = type(iter).__anext__

            header = await anext(iter)
            self.assertEqual(["Column1", "Column2"], header)
            try:
                row = await anext(iter)
            except CsvError as e:
                pass
            except Exception as e:
                self.fail(f"Caught unexpected: {e}")
            else:
                self.fail(f"Got unexpected row: {repr(row)}")



if __name__ == "__main__":
    unittest.main()
