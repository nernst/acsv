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

    async def _run_test(self, reader: Reader, expected: list[list[str]]) -> None:
        async for index, row in aenumerate(reader):
            if index < len(expected):
                self.assertEqual(expected[index], row)
            else:
                self.assertFalse(f"Only expected {len(expected)} rows. {index=}, {row=}")

        self.assertEqual(expected[0], reader.fieldnames)

    async def test_simple_csv(self):
        csv = """
Column1,Column2\r
1,asdf\r
2,hjkl\r
3,"The quick brown fox jumped over the lazy log, and then continued along."\r
4,"This string "" has an embedded quote."\r
5,This row has an extra column,Extra Value\r
6\r
7,"This column has a delimiter, in the middle"\r
""".strip()
        expected = (
            ["Column1", "Column2"],
            ["1", "asdf"],
            ["2", "hjkl"],
            ["3", "The quick brown fox jumped over the lazy log, and then continued along."],
            ["4", "This string \" has an embedded quote."],
            ["5", "This row has an extra column", "Extra Value"],
            ["6"],
            ["7", "This column has a delimiter, in the middle"],
        )
        async with AsyncStringIO(csv, newline="") as fp:
            reader = Reader(fp)
            await self._run_test(reader, expected)

    async def test_simple_tab_csv(self):
        csv = """
Column1\tColumn2\r
1\tasdf\r
2\thjkl\r
3\tThe quick brown fox jumped over the lazy log, and then continued along.\r
4\t"This string "" has an embedded quote."\r
5\tThis row has an extra column\tExtra Value\r
6\r
7\tThis column has a comma, but should not be a separate column
""".strip()
        expected = (
            ["Column1", "Column2"],
            ["1", "asdf"],
            ["2", "hjkl"],
            ["3", "The quick brown fox jumped over the lazy log, and then continued along."],
            ["4", "This string \" has an embedded quote."],
            ["5", "This row has an extra column", "Extra Value"],
            ["6"],
            ["7", "This column has a comma, but should not be a separate column"],
        )
        async with AsyncStringIO(csv, newline='') as fp:
            reader = Reader(fp, dialect="excel-tab")
            await self._run_test(reader, expected)

    async def test_bad_quote(self):
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

    async def test_bad_escape(self):
        test = """
Column1,Column2
123,"This column has a bad \ escape "
""".strip()
        
        async with AsyncStringIO(test, newline='') as fp:
            reader = Reader(fp, escapechar="\\")
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

    async def test_skipinitialwhitespace_on(self):
        test = """
Column1,Column2\r
1,   This column has initial whitespace.
""".strip()
        expected = (
            ["Column1", "Column2"],
            ["1", "This column has initial whitespace."],
        )

        async with AsyncStringIO(test, newline='') as fp:
            reader = Reader(fp, skipinitialspace=True)
            await self._run_test(reader, expected)

    async def test_skipinitialwhitespace_off(self):
        test = """
Column1,Column2\r
1,   This column has initial whitespace.
""".strip()
        expected = (
            ["Column1", "Column2"],
            ["1", "   This column has initial whitespace."],
        )

        async with AsyncStringIO(test, newline='') as fp:
            reader = Reader(fp, skipinitialspace=False)
            await self._run_test(reader, expected)


if __name__ == "__main__":
    unittest.main()
