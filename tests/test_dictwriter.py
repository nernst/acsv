import pathlib
import unittest
from typing import Sequence
from acsv import DictReader, DictWriter
from acsv.util import AsyncStringIO


class DictWriterTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        super().setUp()

        self._samples_dir = pathlib.Path(__file__).parent / "../test_files"

    async def test_simple_case(self) -> None:
        test_csv = """
Column1,Column2\r
1,Test Row 1\r
2,Test Row 2\r
3,"Test Row 3, this has a delimiter"\r
4,"Test Row 4: This row has a double-quote ("") in it"\r
""".strip()

        rows = []
        test_csv_lines = test_csv.split("\n")
        header: Sequence[str]

        async with AsyncStringIO(test_csv, newline="") as fp:
            reader: DictReader = DictReader(fp)
            async for row in reader:
                rows.append(row)
            header = reader.fieldnames

        try:
            self.assertEqual(len(test_csv_lines) - 1, len(rows), "Reading test data didn't produce the expected number of rows.")
        except:
            print(f"{rows=}")
            raise

        async with AsyncStringIO(newline="") as fp:
            writer: DictWriter = DictWriter(fp, fieldnames=header)
            await writer.writeheader()
            await writer.writerows(rows)
            result = await fp.getvalue()

        result_lines = result.split("\n")
        
        self.assertEqual(test_csv_lines, result_lines)
