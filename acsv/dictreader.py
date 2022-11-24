#!/usr/bin/env python3
import csv
from typing import Any, AsyncGenerator, Mapping, Optional, Sequence, TypeVar, Generic
from .exceptions import CsvError
from .reader import Reader
from ._protocols import AsyncFile

RestVal = TypeVar("RestVal")

class DictReader(Generic[RestVal]):
    _fieldnames: Optional[Sequence[str]]
    _restkey: Optional[str]
    _restVal: Optional[RestVal] = None

    RowType = Mapping[str | Any, str | Any]

    @property
    def fieldnames(self) -> Sequence[str]:
        if self._fieldnames is None:
            raise CsvError("When not supplied in constructor, fieldnames is not available until the first row has been read.")
        return self._fieldnames

    def __init__(
        self, 
        csvfile: AsyncFile, 
        fieldnames: Optional[Sequence[str]] = None,
        restkey: Optional[str] = None,
        restval: Optional[RestVal] = None,
        dialect: str | csv.Dialect = "excel",
        **kwargs,
    ) -> None:
        self._reader = Reader(csvfile, dialect=dialect, **kwargs)
        self._fieldnames = fieldnames
        self._restkey = restkey
        self._restval = restval

    def _make_dict(self, row: Sequence[str]) -> RowType:
        assert self._fieldnames is not None
        if len(row) >= len(self._fieldnames):
            d: dict[str | Any, str | Any]= dict(zip(self._fieldnames, row))
            if len(row) > len(self._fieldnames):
                remain = row[len(self._fieldnames):]
                d[self._restkey] = list(remain)
            return d
        else:
            return {f: row[index] if index < len(row) else self._restval for index, f in enumerate(self._fieldnames)}


    async def __aiter__(self) -> AsyncGenerator[RowType, None]:
        async for row in self._reader:
            if self._fieldnames is None:
                self._fieldnames = row
            else:
                yield self._make_dict(row)
    
