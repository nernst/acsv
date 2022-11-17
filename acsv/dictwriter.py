import csv as _csv
from typing import Any, Iterable, Literal, Mapping, Optional, Sequence, TypeAlias
from ._protocols import AsyncFile
from .exceptions import CsvError
from .writer import Writer


class DictWriter:

    RowType: TypeAlias = Mapping[str | Any, str | Any]

    def __init__(
        self,
        f: AsyncFile, 
        fieldnames: Sequence[str], 
        restval: str = "", 
        extrasaction: Literal["raise"] | Literal["ignore"] = "raise",
        dialect: str | _csv.Dialect = "excel"
    ):
        self._writer = Writer(f, dialect)
        self._fieldnames = fieldnames
        self._restval = restval
        self._extrasaction = extrasaction

    async def writerow(self, row: RowType) -> Optional[int]:
        row = dict(row)
        line = []

        for key in self._fieldnames:
            line.append(row.pop(key, self._restval))
        
        if self._extrasaction == "raise" and row:
            raise CsvError(f"Unused keys in row: {row.keys()}")

        return await self._writer.writerow(line)

    async def writerows(self, rows: Iterable[RowType]) -> None:
        for row in rows:
            await self.writerow(row)

    async def writeheader(self) -> Optional[int]:
        return await self._writer.writerow(self._fieldnames)
    