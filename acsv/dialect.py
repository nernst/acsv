import csv
import _csv

class _Dialect(csv.Dialect):
    delimiter: str
    quotechar: str | None
    escapechar: str | None
    doublequote: bool
    skipinitialspace: bool
    lineterminator: str
    quoting: int

    def __init__(self, base: csv.Dialect, **kwargs):
        self.delimiter = base.delimiter
        self.quotechar = base.quotechar
        self.escapechar = base.escapechar
        self.doublequote = base.doublequote
        self.skipinitialspace = base.skipinitialspace
        self.lineterminator = base.lineterminator
        self.quoting = base.quoting

        if delimeter := kwargs.get("delimiter"):
            self.delimiter = str(delimeter)
        if "quotechar" in kwargs:
            quotechar = kwargs.get("quotechar")
            self.quotechar = str(quotechar)
        if "escapechar" in kwargs:
            escapechar = kwargs.get("escapechar")
            self.escapechar = str(escapechar) if escapechar else None
        if doublequote := kwargs.get("doublequote"):
            self.doublequote = bool(doublequote)
        if skipinitialspace := kwargs.get("skipinitialspace"):
            self.skipinitialspace = bool(skipinitialspace)
        if lineterminator := kwargs.get("lineterminator"):
            self.lineterminator = str(lineterminator)
        if quoting := kwargs.get("lineterminator"):
            self.quoting = int(quoting)


def get_dialect(dialect: str | csv.Dialect, **kwargs) -> _Dialect:
    base = csv.get_dialect(dialect) if isinstance(dialect, str) else dialect
    return _Dialect(base, **kwargs)
