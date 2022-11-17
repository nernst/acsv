import csv as _csv
from io import StringIO
from typing import Iterable, Optional, TypeAlias, Union
from .exceptions import CsvError
from .util import none_throws
from ._protocols import AsyncFile

# TODO: handle complex numbers?

class Writer:
    dialect: _csv.Dialect

    ValueType: TypeAlias = Union[str, float, int]
    RowType: TypeAlias = Iterable[ValueType]

    def __init__(self, csvfile: AsyncFile, dialect: str | _csv.Dialect = "excel") -> None:
        self._csvfile = csvfile
        self.dialect = _csv.get_dialect(dialect) if isinstance(dialect, str) else dialect
        self._escape_doublequote = None if self.dialect.quotechar is None else self.dialect.quotechar * 2
        self._escape_quote_sequence = None if self.dialect.escapechar is None else self.dialect.escapechar + none_throws(self.dialect.quotechar)
        self._escape_delimiter_sequence = None if self.dialect.escapechar is None else self.dialect.escapechar + none_throws(self.dialect.delimiter)
        doublequote = self.dialect.doublequote
        self._needs_newline = False

        match self.dialect.quoting:
            case _csv.QUOTE_ALL if doublequote:
                self._quote = self._quote_all_doublequote
            
            case _csv.QUOTE_ALL if not doublequote:
                self._quote = self._quote_all_escape

            case _csv.QUOTE_MINIMAL if doublequote:
                self._quote = self._quote_minimal_doublequote

            case _csv.QUOTE_MINIMAL if not doublequote:
                self._quote = self._quote_minimal_escape

            case _csv.QUOTE_NONNUMERIC if doublequote:
                self._quote = self._quote_nonnumeric_doublequote
            
            case _csv.QUOTE_NONNUMERIC if not doublequote:
                self._quote = self._quote_nonnumeric_escape
        
            case _csv.QUOTE_NONE:
                self._quote = self._quote_none

            case _:
                raise CsvError(f"Unsupported quoting: {self.dialect.quoting} {doublequote=}")

    def _quote_all_doublequote(self, value: str | int | float) -> str:
        quotechar = none_throws(self.dialect.quotechar)
        value = str(value)
        value = value.replace(quotechar, none_throws(self._escape_doublequote))
        return "".join([quotechar, value, quotechar])

    def _quote_all_escape(self, value: str | int | float) -> str:
        quotechar = none_throws(self.dialect.quotechar)
        value = str(value)
        if quotechar in value:
            escapeseq = none_throws(self._escape_doublequote)
            value = value.replace(quotechar, escapeseq)
        return "".join([quotechar, value, quotechar])

    def _quote_minimal_doublequote(self, value: str | int | float) -> str:
        quotechar = none_throws(self.dialect.quotechar)
        value = str(value)
        need_quotes = False
        if quotechar in value:
            need_quotes = True
            escapeseq = none_throws(self._escape_doublequote)
            value = value.replace(quotechar, escapeseq)

        if self.dialect.delimiter in value:
            need_quotes = True

        if need_quotes:
            return "".join([quotechar, value, quotechar])
        
        return value

    def _quote_minimal_escape(self, value: str | int | float) -> str:
        quotechar = none_throws(self.dialect.quotechar)
        value = str(value)
        need_quotes = False
        if quotechar in value:
            escapeseq = none_throws(self._escape_quote_sequence)
            value = value.replace(quotechar, escapeseq)

        if self.dialect.delimiter in value:
            need_quotes = True

        if need_quotes:
            return "".join([quotechar, value, quotechar])
        
        return value

    def _quote_nonnumeric_doublequote(self, value: str | int | float) -> str:
        if isinstance(value, str):
            return self._quote_all_doublequote(value)
        else:
            return self._quote_minimal_escape(value)

    def _quote_nonnumeric_escape(self, value: str | int | float) -> str:
        if isinstance(value, str):
            return self._quote_all_escape(value)
        else:
            return self._quote_minimal_escape(value)

    def _quote_none(self, value: str | int | float) -> str:
        value = str(value)
        delimiter = self.dialect.delimiter
        if delimiter in value:
            escapeseq = none_throws(self._escape_delimiter_sequence)
            return value.replace(delimiter, escapeseq)
        return value

    def _formatrow(self, row: RowType) -> str:
        delimiter = self.dialect.delimiter
        line = StringIO()
        if self._needs_newline:
            line.write(self.dialect.lineterminator)

        iter_ = iter(row)

        try:
            value = next(iter_)
            line.write(self._quote(value))
        except StopIteration:
            return line.getvalue()
        
        while True:
            try:
                value = self._quote(next(iter_))
                line.write(delimiter)
                line.write(value)
            except StopIteration:
                break
        
        return line.getvalue()
    
    async def writerow(self, row: RowType) -> Optional[int]:
        res = await self._csvfile.write(self._formatrow(row))
        self._needs_newline = True
        return res

    async def writerows(self, rows: Iterable[RowType]) -> None:
        for row in rows:
            await self.writerow(row)
