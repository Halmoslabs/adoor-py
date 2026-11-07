"""Turn Python values into the parameters ADoor accepts, and result text back into Python values.

ADoor parameters are strings, finite numbers, booleans or null. PostgreSQL casts a string
to the column's type, so dates, decimals, JSON and arrays travel as their text forms.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import re
import uuid
from decimal import Decimal
from typing import Any, Callable, Dict, Optional

_MAX_SAFE_INTEGER = 2**53 - 1


class Json:
    """Marks a value for a json or jsonb column. A plain list is sent as a PostgreSQL array instead."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Json({self.value!r})"


def _float_param(value: float) -> Any:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return value


def _array_item(value: Any) -> str:
    if value is None:
        return "NULL"
    text = encode_param(value)
    if isinstance(text, bool):
        return "true" if text else "false"
    text = str(text)
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _missing(value: Any) -> bool:
    """pandas.NA, pandas.NaT and numpy datetime NaT, without importing either library."""
    kind = type(value).__name__
    return kind in ("NAType", "NaTType") or (kind == "datetime64" and str(value) == "NaT")


def encode_cell(value: Any) -> Any:
    """One DataFrame or record cell. NaN means missing here, as in pandas.to_sql: it is sent as NULL."""
    if isinstance(value, float) and math.isnan(value):
        return None
    if type(value).__name__ in ("float64", "float32", "float16") and value != value:
        return None
    return encode_param(value)


def encode_param(value: Any) -> Any:
    """One Python value as an ADoor parameter."""
    if value is None or _missing(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value if -_MAX_SAFE_INTEGER <= value <= _MAX_SAFE_INTEGER else str(value)
    if isinstance(value, float):
        return _float_param(value)
    if isinstance(value, str):
        return value
    if isinstance(value, Decimal):
        return "NaN" if value.is_nan() else str(value)
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            raise ValueError(
                "A datetime without a time zone is ambiguous. Pass an aware datetime, such as "
                "datetime.now(timezone.utc), or localize a pandas column with .dt.tz_localize('UTC')."
            )
        return value.isoformat()
    if isinstance(value, (_dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Json):
        return json.dumps(value.value, default=_json_default, allow_nan=False)
    if isinstance(value, dict):
        return json.dumps(value, default=_json_default, allow_nan=False)
    item = getattr(value, "item", None)
    if callable(item) and getattr(value, "shape", None) == ():
        return encode_param(item())
    tolist = getattr(value, "tolist", None)
    if callable(tolist) and not isinstance(value, (list, tuple)):
        value = tolist()
    if isinstance(value, (list, tuple)):
        return "{" + ",".join(_array_item(item) for item in value) + "}"
    raise TypeError(f"ADoor cannot send a {type(value).__name__} as a parameter.")


def _json_default(value: Any) -> Any:
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return tolist()
    return encode_param(value)


_ZONE = re.compile(r"([+-]\d\d)(\d\d)?$")


def parse_timestamp(text: str) -> _dt.datetime:
    value = text.strip().replace(" ", "T", 1)
    match = _ZONE.search(value)
    if match and "T" in value and len(value) > 10:
        value = value[: match.start()] + match.group(1) + ":" + (match.group(2) or "00")
    head, dot, rest = value.partition(".")
    if dot:
        digits = re.match(r"\d+", rest)
        fraction = digits.group(0) if digits else ""
        value = head + "." + fraction[:6].ljust(6, "0") + rest[len(fraction):]
    return _dt.datetime.fromisoformat(value)


def _numeric(text: Any, as_decimal: bool) -> Any:
    if text is None or isinstance(text, (int, float)) and not as_decimal:
        return text
    return Decimal(str(text)) if as_decimal else float(text)


def _scalar(type_oid: str, as_decimal: bool) -> Optional[Callable[[Any], Any]]:
    if type_oid in ("20", "21", "23"):
        return lambda value: int(value) if isinstance(value, str) else value
    if type_oid in ("700", "701", "1700"):
        return lambda value: _numeric(value, as_decimal)
    if type_oid in ("1114", "1184"):
        return lambda value: parse_timestamp(value) if isinstance(value, str) else value
    if type_oid == "1082":
        return lambda value: _dt.date.fromisoformat(value) if isinstance(value, str) else value
    return None


_ARRAY_OF = {"1016": "20", "1005": "21", "1007": "23", "1021": "700", "1022": "701", "1231": "1700",
             "1115": "1114", "1185": "1184", "1182": "1082"}


def decoders(columns: list, as_decimal: bool = False) -> Dict[str, Callable[[Any], Any]]:
    """Per-column converters from the result's PostgreSQL type ids. Text, uuid, boolean and JSON need none."""
    found: Dict[str, Callable[[Any], Any]] = {}
    for column in columns or []:
        type_oid = str(column.get("type", ""))
        convert = _scalar(type_oid, as_decimal)
        if convert is None and type_oid in _ARRAY_OF:
            inner = _scalar(_ARRAY_OF[type_oid], as_decimal)
            if inner is not None:
                convert = (lambda each: lambda value: None if value is None else
                           [None if item is None else each(item) for item in value])(inner)
        if convert is not None:
            found[column["name"]] = convert
    return found


def decode_rows(rows: list, columns: list, as_decimal: bool = False) -> list:
    convert = decoders(columns, as_decimal)
    if not convert:
        return rows
    decoded = []
    for row in rows:
        copy = dict(row)
        for name, change in convert.items():
            if copy.get(name) is not None:
                copy[name] = change(copy[name])
        decoded.append(copy)
    return decoded
