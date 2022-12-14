import pathlib
import unittest
from acsv import Reader, Writer
from acsv.util import AsyncStringIO


class WriterTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        super().setUp()

        self._samples_dir = pathlib.Path(__file__).parent / "../test_files"

    async def test_simple_case(self) -> None:
        test_csv = """
Column1,Column2\r
1,Test Row 1\r
2,Test Row 2\r
3,"Test Row 3, this has a delimiter"\r
4,Test Row 4: This row has an extra value,Extra Value\r
5,"Test Row 5: This row has a double-quote ("") in it"\r
""".strip()

        rows = []
        test_csv_lines = test_csv.split("\n")

        async with AsyncStringIO(test_csv, newline="") as fp:
            reader = Reader(fp)
            async for row in reader:
                rows.append(row)
        try:
            self.assertEqual(len(test_csv_lines), len(rows), "Reading test data didn't produce the expected number of rows.")
        except:
            print(f"{rows=}")
            raise

        async with AsyncStringIO(newline="") as fp:
            writer = Writer(fp)
            await writer.writerows(rows)
            result = await fp.getvalue()

        result_lines = result.split("\n")
        
        self.assertEqual(test_csv_lines, result_lines)