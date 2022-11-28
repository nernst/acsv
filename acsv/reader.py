#!/usr/bin/env python3
import asyncio
import csv as _csv
import io
import sys
from enum import Enum, auto
from typing import cast, AsyncGenerator, Awaitable, Iterable, Optional, Sequence, Tuple, TypeVar
from .exceptions import CsvError
from ._protocols import AsyncFile
from .dialect import get_dialect

assert sys.version_info >= (3, 10)


class _Token(Enum):
    CHAR = auto()
    DELIMITER = auto()
    QUOTE = auto()
    ESCAPE_CHAR = auto()
    CR = auto()
    LF = auto()
    EOF = auto()
    WS = auto()


class _Scanner:
    dialect: _csv.Dialect
    csvfile: AsyncFile
    buffer_size: int
    buffer: Optional[str]
    pos: int = 0
    _next_fill: Optional[asyncio.Task[str]]
    line_start: int = 0
    line: int = 1
    bpos: int = 0

    Self = TypeVar("Self", bound="_Scanner")

    @property
    def current(self) -> str:
        if self.buffer is None:
            return ""
        return self.buffer[self.bpos]

    def __init__(self, dialect: _csv.Dialect, csvfile: AsyncFile, buffer_size: int = 4096) -> None:
        self.dialect = dialect
        self.csvfile = csvfile
        self.buffer_size = buffer_size
        self.buffer = None

    async def __aenter__(self: Self) -> Self:
        self._next_fill = asyncio.create_task(self.csvfile.read(self.buffer_size))
        return self

    async def __aexit__(self, *_) -> None:
        if self._next_fill is not None:
            self._next_fill.cancel()
            try:
                await self._next_fill
            except asyncio.exceptions.CancelledError:
                pass
            self._next_fill = None

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

    async def __aiter__(self) -> AsyncGenerator[Tuple[_Token, str, int, int], None]:
        dialect = self.dialect
        while True:
            if not await self._advance():
                yield _Token.EOF, "", self.line, self.pos - self.line_start
                break

            char = self.current
            token: _Token
            match (char):
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
                case ' ':
                    token = _Token.WS

                case _:
                    token = _Token.CHAR

            yield token, char, self.line, self.pos - self.line_start


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

    def __init__(
            self, 
            csvfile: AsyncFile, 
            dialect: str | _csv.Dialect = "excel",
            **kwargs,
        ) -> None:
        self._csvfile = csvfile
        self.dialect = get_dialect(dialect, **kwargs)

    async def __aiter__(self) -> AsyncGenerator[Sequence[str], None]:
        line: list[str] = []
        field = io.StringIO()
        state = _State.BEGIN_FIELD

        def emit(character: str) -> None:
            nonlocal state, field
            field.write(character)
            if state == _State.BEGIN_FIELD:
                state = _State.FIELD

        def new_field() -> None:
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

        def bad_state(token, char, line_no, col, reason: Optional[str] = None):
            nonlocal state
            raise CsvError(f"Bad Format: {token} {char=} @ {line_no=}:{col=} {state=} {reason=}")

        async with _Scanner(self.dialect, self._csvfile) as scanner:
            async for token, char, line_no, col in scanner:
                print(f"{token=} {char=}, {line_no=}, {col=}")
                match token:
                    case _Token.CHAR if state == _State.EXPECT_QUOTE_OR_FIELD_TERM:
                        bad_state(token, char, line_no, col)

                    case _Token.CHAR:
                        emit(char)

                    case _Token.DELIMITER if state in (_State.QUOTED_FIELD, _State.ESCAPE):
                        emit(self.dialect.delimiter)

                    case _Token.DELIMITER if state == _State.EXPECT_QUOTE_OR_FIELD_TERM:
                        new_field()

                    case _Token.DELIMITER:
                        new_field()

                    case _Token.QUOTE if state == _State.BEGIN_FIELD:
                        state = _State.QUOTED_FIELD
                    
                    case _Token.QUOTE if state == _State.FIELD:
                        bad_state(token, char, line_no, col)

                    case _Token.QUOTE if state == _State.QUOTED_FIELD:
                        state = _State.EXPECT_QUOTE_OR_FIELD_TERM

                    case _Token.QUOTE if state == _State.EXPECT_QUOTE_OR_FIELD_TERM:
                        state = _State.QUOTED_FIELD
                        field.write(char)

                    case _Token.ESCAPE_CHAR if self.dialect.escapechar and state == _State.ESCAPE:
                        field.write(self.dialect.escapechar)

                    case _Token.ESCAPE_CHAR if self.dialect.escapechar and state != _State.ESCAPE:
                        state = _State.ESCAPE

                    case _Token.CR:
                        continue

                    case _Token.LF:
                        self.line_num += 1
                        if state == _State.QUOTED_FIELD:
                            field.write("\n")
                        else:
                            new_field()
                            for l in new_line():
                                yield l

                    case _Token.WS if self.dialect.skipinitialspace and state == _State.BEGIN_FIELD:
                        pass
                
                    case _Token.WS:
                        emit(char)

                    case _Token.EOF:
                        new_field()
                        for l in new_line():
                            yield l
                        return

                    case _:
                        bad_state(token, char, line_no, col, "Unknown token")
    