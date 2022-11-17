#!/usr/bin/env python3
import asyncio
import csv as _csv
import io
import sys
from enum import Enum, auto
from typing import cast, AsyncGenerator, Awaitable, Iterable, Optional, Sequence, Tuple
from .exceptions import CsvError
from ._protocols import AsyncFile

assert sys.version_info >= (3, 10)


class _Token(Enum):
    CHAR = auto()
    DELIMITER = auto()
    QUOTE = auto()
    ESCAPE_CHAR = auto()
    CR = auto()
    LF = auto()
    EOF = auto()


class _Scanner:
    dialect: _csv.Dialect
    csvfile: AsyncFile
    buffer_size: int
    buffer: Optional[str]
    pos: int = 0
    _next_fill = Optional[Awaitable[str]]
    line_start: int = 0
    line: int = 1
    bpos: int = 0

    @property
    def current(self) -> Optional[str]:
        if self.buffer is None:
            return None
        return self.buffer[self.bpos]

    def __init__(self, dialect: _csv.Dialect, csvfile: AsyncFile, buffer_size: int = 8192) -> None:
        self.dialect = dialect
        self.csvfile = csvfile
        self.buffer_size = buffer_size
        self.buffer = None
        self._next_fill = asyncio.create_task(self.csvfile.read(self.buffer_size))

    async def _advance(self) -> bool:
        if self.buffer is None:
            return await self._fill()
        else:
            self.pos += 1
            self.bpos += 1
            if self.bpos == len(self.buffer):
                return await self._fill()
            return True

    async def _fill(self) -> bool:
        if self._next_fill is None:
            return False

        self.bpos = 0
        self.buffer = await cast(Awaitable[str], self._next_fill)
        if len(self.buffer) == 0:
            self._next_fill = None
            return False
        else:
            self._next_fill = asyncio.create_task(self.csvfile.read(self.buffer_size)) 
            return True

    async def __aiter__(self) -> AsyncGenerator[Tuple[_Token, Optional[str], int, int], None]:
        dialect = self.dialect
        while True:
            if not await self._advance():
                yield _Token.EOF, None, self.line, self.pos - self.line_start
                break

            c = self.current
            token: _Token
            match (c):
                case (dialect.quotechar):
                    token = _Token.QUOTE
                case (dialect.escapechar) if dialect.escapechar:
                    token = _Token.ESCAPE_CHAR
                case (None):
                    token = _Token.EOF
                case '\r':
                    token = _Token.CR
                case '\n':
                    self.line += 1
                    self.line_start = self.pos
                    token = _Token.LF
                case (dialect.delimiter):
                    token = _Token.DELIMITER
                case _:
                    token = _Token.CHAR

            yield token, c, self.line, self.pos - self.line_start


class _State(Enum):
    BEGIN_FIELD = auto()
    FIELD = auto()
    QUOTED_FIELD = auto()
    EXPECT_QUOTE_OR_FIELD_TERM = auto()
    ESCAPE = auto()


class Reader:
    _csvfile: AsyncFile
    dialic: _csv.Dialect
    line_num: int = 0
    _buffer: str
    _pos: int = 0
    _fieldnames: Optional[Sequence[str]] = None

    @property
    def fieldnames(self) -> Sequence[str]:
        if self._fieldnames is None:
            raise CsvError("fieldnames is not available until first row has been read.")
        return self._fieldnames

    def __init__(self, csvfile: AsyncFile, dialect: str | _csv.Dialect = "excel") -> None:
        self._csvfile = csvfile
        self.dialect = _csv.get_dialect(dialect) if isinstance(dialect, str) else dialect

    async def __aiter__(self) -> AsyncGenerator[Sequence[str], None]:
        line: list[str] = []
        field = io.StringIO()
        state = _State.BEGIN_FIELD

        def new_field():
            nonlocal line, field, state
            line.append(field.getvalue())
            field.truncate(0)
            field.seek(0)
            state = _State.BEGIN_FIELD

        def new_line() -> Iterable[Sequence[str]]:
            nonlocal line, self
            if self._fieldnames is None:
                self._fieldnames = line

            if line:
                yield line
            line = []

        async for token, c, line_no, col in _Scanner(self.dialect, self._csvfile):
            # print(f"{token=} {c=}, {line_no=}, {col=}")
            match token:
                case _Token.CHAR if c is not None:
                    field.write(c)

                case _Token.DELIMITER if state == _State.QUOTED_FIELD:
                    field.write(self.dialect.delimiter)

                case _Token.DELIMITER if state == _State.ESCAPE:
                    field.write(self.dialect.delimiter)

                case _Token.DELIMITER if state == _State.EXPECT_QUOTE_OR_FIELD_TERM:
                    new_field()

                case _Token.DELIMITER:
                    new_field()

                case _Token.QUOTE if state == _State.BEGIN_FIELD:
                    state = _State.QUOTED_FIELD
                
                case _Token.QUOTE if state == _State.FIELD:
                    raise CsvError(f"Unexpected {c} @ {line_no=}:{col=}")
                
                case _Token.QUOTE if state == _State.QUOTED_FIELD:
                    state = _State.EXPECT_QUOTE_OR_FIELD_TERM

                case _Token.QUOTE if state == _State.EXPECT_QUOTE_OR_FIELD_TERM:
                    assert c is not None
                    state = _State.QUOTED_FIELD
                    field.write(c)

                case _Token.ESCAPE_CHAR if self.dialect.escapechar and state == _State.ESCAPE:
                    field.write(self.dialect.escapechar)

                # probably insufficient
                case _Token.ESCAPE_CHAR if self.dialect.escapechar and state != _State.ESCAPE:
                    state = _State.ESCAPE

                case _Token.CR:
                    continue

                case _Token.LF:
                    if state == _State.QUOTED_FIELD:
                        field.write("\n")
                    else:
                        new_field()
                        for l in new_line():
                            yield l

                case _Token.EOF:
                    new_field()
                    for l in new_line():
                        yield l
                    return

                case _:
                    raise CsvError(f"Unknown token: {token} @ {line_no=}:{col=} {state=}")
        