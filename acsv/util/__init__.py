import asyncio
import io
import sys
from typing import AsyncIterable, AsyncIterator, Literal, Optional, Tuple, TypeAlias, TypeVar, Union

assert sys.version_info >= (3, 10)

# lifted from _typeshed.builtins
OpenTextModeUpdating: TypeAlias = Literal[
    "r+",
    "+r",
    "rt+",
    "r+t",
    "+rt",
    "tr+",
    "t+r",
    "+tr",
    "w+",
    "+w",
    "wt+",
    "w+t",
    "+wt",
    "tw+",
    "t+w",
    "+tw",
    "a+",
    "+a",
    "at+",
    "a+t",
    "+at",
    "ta+",
    "t+a",
    "+ta",
    "x+",
    "+x",
    "xt+",
    "x+t",
    "+xt",
    "tx+",
    "t+x",
    "+tx",
]
OpenTextModeWriting: TypeAlias = Literal["w", "wt", "tw", "a", "at", "ta", "x", "xt", "tx"]
OpenTextModeReading: TypeAlias = Literal["r", "rt", "tr", "U", "rU", "Ur", "rtU", "rUt", "Urt", "trU", "tUr", "Utr"]
OpenTextMode: TypeAlias = Union[OpenTextModeUpdating, OpenTextModeWriting, OpenTextModeReading]


AsyncTextFileSelf = TypeVar("AsyncTextFileSelf", bound="AsyncTextFile")

class AsyncTextFile:
    """
    Simple async wrapper around text based files for testing.
    """
    _file: io.TextIOBase

    def __init__(self, filename: str, mode: OpenTextMode = "r") -> None: 
        self.filename = filename
        self.mode = mode

    async def __aenter__(self: AsyncTextFileSelf) -> AsyncTextFileSelf:
        self._file = await asyncio.to_thread(open, self.filename, self.mode)
        return self

    async def __aexit__(self, *args) -> None:
        close = type(self._file).__exit__
        await asyncio.to_thread(close, self._file, *args)

    async def read(self, size: int) -> str:
        return await asyncio.to_thread(self._file.read, size)
    
    async def write(self, data: str) -> int:
        return await asyncio.to_thread(self._file.write, data)




class AsyncStringIO:
    """
    A completely unnecessary class that adds async to StringIO to be able to use
    in memory async buffers for testing.
    """

    _file: io.StringIO

    Self = TypeVar("Self", bound="AsyncStringIO")

    def __init__(self, initial_value: str = "", newline="\n") -> None:
        self._file = io.StringIO(initial_value=initial_value, newline=newline)

    async def __aenter__(self: Self) -> Self:
        type(self._file).__enter__(self._file)
        return self

    async def __aexit__(self, *args):
        type(self._file).__exit__(self._file, *args)

    async def read(self, size: int = -1) -> str:
        return await asyncio.to_thread(self._file.read, size)

    async def readline(self, size: int = -1) -> str:
        return await asyncio.to_thread(self._file.readline, size)

    async def write(self, value: str) -> int:
        return await asyncio.to_thread(self._file.write, value)

    async def getvalue(self) -> str:
        return await asyncio.to_thread(self._file.getvalue)


T = TypeVar("T")

async def aenumerate(iterable: AsyncIterable[T], *, start: int = 0) -> AsyncIterator[Tuple[int, T]]:
    async for value in iterable:
        yield start, value
        start += 1


def none_throws(value: Optional[T]) -> T:
    if value is None:
        raise ValueError(value)
    return value