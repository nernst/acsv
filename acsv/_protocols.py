from typing import Optional, Protocol

class AsyncFile(Protocol):
    async def read(self, size: int = -1, /) -> str:
        ...

    async def write(self, value: str) -> Optional[int]:
        ...
