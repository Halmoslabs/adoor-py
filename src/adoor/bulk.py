"""Large loads: stage a CSV file, stream it to ADoor, and COPY it into a table.

The server reads the file in chunks and commits each chunk with its history, so a load of millions of rows
needs neither a large request nor client memory. A dropped connection keeps the committed chunks, and the
same COPY statement with the same idempotency key continues from where it stopped.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from typing import TYPE_CHECKING, Any, BinaryIO, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from .errors import AdoorError, error_from_body
from .values import encode_cell

if TYPE_CHECKING:  # pragma: no cover
    from .client import Client

Source = Union[str, "os.PathLike[str]", bytes, BinaryIO]
_NAME = re.compile(r"[^A-Za-z0-9._-]+")
_RESUMABLE = frozenset({"COPY_RESUME", "NETWORK"})


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _csv_cell(value: Any) -> str:
    """One field. NULL is an empty unquoted field; every string is quoted, so an empty string stays empty."""
    value = encode_cell(value)
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value)
    return '"' + text.replace('"', '""') + '"'


def write_csv(records: Iterable[Mapping[str, Any]], names: Sequence[str], out: BinaryIO) -> int:
    """Write records as CSV with a header row; returns the number of data rows."""
    out.write((",".join(_quote(name) for name in names) + "\n").encode("utf-8"))
    count = 0
    lines: List[str] = []
    for record in records:
        lines.append(",".join(_csv_cell(record.get(name)) for name in names))
        count += 1
        if len(lines) >= 10_000:
            out.write(("\n".join(lines) + "\n").encode("utf-8"))
            lines = []
    if lines:
        out.write(("\n".join(lines) + "\n").encode("utf-8"))
    return count


def _frame_records(frame: Any) -> Iterable[Dict[str, Any]]:
    columns = [str(name) for name in frame.columns]
    for row in frame.itertuples(index=False, name=None):
        yield dict(zip(columns, row))


def _open(source: Source) -> tuple:
    """A readable binary stream, its size, and whether this function opened it."""
    if isinstance(source, (bytes, bytearray)):
        return io.BytesIO(bytes(source)), len(source), True
    if isinstance(source, (str, os.PathLike)):
        handle = open(source, "rb")
        return handle, os.fstat(handle.fileno()).st_size, True
    position = source.tell()
    source.seek(0, os.SEEK_END)
    size = source.tell() - position
    source.seek(position)
    return source, size, False


def _header(stream: BinaryIO) -> List[str]:
    start = stream.tell()
    line = stream.readline().decode("utf-8-sig")
    stream.seek(start)
    return next(csv.reader([line])) if line.strip() else []


def _upload(client: "Client", url: str, stream: BinaryIO, size: int, attempts: int) -> None:
    """PUT the file. Until COPY starts the same transfer accepts the bytes again, so a failed upload is retried."""
    start = stream.tell()
    for attempt in range(attempts):
        stream.seek(start)
        request = urllib.request.Request(url, data=stream, method="PUT",
                                         headers={"Content-Type": "text/csv", "Content-Length": str(size)})
        try:
            with client._opener.open(request, timeout=max(60.0, client.timeout)) as response:
                response.read()
            return
        except urllib.error.HTTPError as error:
            with error:
                failure = error_from_body(error.code, error.read())
            if not failure.retryable or attempt + 1 >= attempts:
                raise failure from None
        except (urllib.error.URLError, OSError) as error:
            if attempt + 1 >= attempts:
                raise AdoorError("NETWORK", f"The upload could not reach {client.url}: {getattr(error, 'reason', error)}", 0) from error
        time.sleep(min(30.0, 0.5 * 2 ** attempt))


def import_csv(client: "Client", table: str, source: Source, *, columns: Optional[Sequence[str]] = None,
               header: bool = True, key: Optional[str] = None, filename: Optional[str] = None,
               space: Optional[str] = None, workspace: Optional[str] = None, attempts: int = 10,
               request_timeout: float = 900.0) -> Dict[str, Any]:
    """Load a CSV file, bytes, or binary stream into ``table`` with ``COPY FROM``.

    With ``header=True`` (default) the first line names the columns, in any order. Each chunk the server
    commits is one change in history. If the connection drops or a request times out, the same statement and
    key resume after the last committed chunk, up to ``attempts`` requests.
    """
    stream, size, opened = _open(source)
    try:
        names = list(columns) if columns is not None else (_header(stream) if header else [])
        digest = hashlib.sha256()
        start = stream.tell()
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
        stream.seek(start)
        base = filename or (os.path.basename(str(source)) if isinstance(source, (str, os.PathLike)) else "import.csv")
        name = _NAME.sub("-", base)[:120] or "import.csv"
        staged = client.execute(f"SELECT adoor_file('import', '{name}')", space=space, workspace=workspace)[0]
        transfer = staged["transfer_id"]
        upload_started = time.perf_counter()
        _upload(client, staged["upload_url"], stream, size, attempts)
        uploaded = time.perf_counter() - upload_started
    finally:
        if opened:
            stream.close()
    column_list = f" ({', '.join(_quote(column) for column in names)})" if names else ""
    options = "FORMAT csv, HEADER true" if header else "FORMAT csv"
    statement = f"COPY {_quote(table)}{column_list} FROM 'adoor://{transfer}' WITH ({options})"
    idempotency = key or str(uuid.uuid4())
    copy_started = time.perf_counter()
    for attempt in range(1, attempts + 1):
        try:
            # This loop is the retry for a dropped COPY, so the client retries only refusals that ran nothing.
            body = client._target({"sql": statement, "idempotencyKey": idempotency}, space, workspace)
            response = client.call("data.execute", body, safe_to_retry=False, timeout=request_timeout)
            rows = response.get("rows") if isinstance(response, dict) else None
            row = rows[0] if rows else {}
            return {"rowCount": int(row.get("committed_rows") or 0), "rowsThisRequest": int(row.get("rows_this_request") or 0),
                    "transferId": transfer, "key": idempotency, "attempts": attempt, "bytes": size,
                    "sha256": digest.hexdigest(), "uploadSeconds": round(uploaded, 3),
                    "copySeconds": round(time.perf_counter() - copy_started, 3)}
        except AdoorError as error:
            if (error.code not in _RESUMABLE and not error.retryable) or attempt == attempts:
                raise
            # Each attempt keeps the chunks it committed, so the next one continues soon after a drop.
            time.sleep(min(5.0, 0.25 * 2 ** min(attempt - 1, 5)))
    raise AdoorError("COPY_RESUME", "The import did not finish.", 409)  # pragma: no cover


def import_frame(client: "Client", table: str, rows: Any, *, create: bool = False,
                 space: Optional[str] = None, workspace: Optional[str] = None, **options: Any) -> Dict[str, Any]:
    """Load a pandas DataFrame, or an iterable of dicts, through a temporary CSV file and ``COPY FROM``.

    NaN and None become NULL, as in ``insert``; strings are quoted, so an empty string stays empty. With
    ``create=True`` the table is created from the DataFrame's dtypes first.
    """
    if create:
        client.create_table(table, rows, space=space, workspace=workspace)
    if hasattr(rows, "columns") and hasattr(rows, "itertuples"):
        names = [str(name) for name in rows.columns]
        records: Iterable[Mapping[str, Any]] = _frame_records(rows)
    else:
        records = list(rows)
        names = []
        for record in records:
            for name in record:
                if name not in names:
                    names.append(name)
    with tempfile.TemporaryFile() as handle:
        count = write_csv(records, names, handle)
        handle.seek(0)
        result = import_csv(client, table, handle, columns=names, filename=f"{table}.csv", space=space,
                            workspace=workspace, **options)
    result["rowsWritten"] = count
    return result
