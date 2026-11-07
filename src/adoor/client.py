"""The ADoor client: SQL, history, undo and identities over the same operation endpoint as the web app, CLI and MCP."""

from __future__ import annotations

import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Union

from .errors import AdoorError, error_from_body
from .values import decode_rows, encode_cell, encode_param

__all__ = ["Client", "Logger", "Result", "Sandbox", "connect", "DEFAULT_URL"]

DEFAULT_URL = "https://app.adoor.run"
VERSION = "0.1.2"
_OPERATION = re.compile(r"^[a-z]+\.[a-z_]+$")
_MAX_PARAMETERS = 65_535
_CHUNK_BYTES = 4 * 1024 * 1024
# Measured on a local server: 10,000 rows took 2.2 s in 1,000-row statements and 6.1 s in one statement.
_CHUNK_ROWS = 1000
_LOOPBACK = ("localhost", "127.0.0.1", "[::1]", "::1")
# Refused before the statement ran: a rate limit, a full SQL queue, or a full request queue.
_NOT_STARTED = frozenset({"RATE_LIMITED", "QUERY_BUSY", "QUEUE_FULL"})
TokenSource = Union[str, Callable[[], str]]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _origin(url: str) -> str:
    parsed = urllib.parse.urlsplit(url.strip())
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise AdoorError("INVALID_URL", "Use a complete server URL, such as https://app.adoor.run.", 400)
    if parsed.username or parsed.password:
        raise AdoorError("INVALID_URL", "The server URL must not contain credentials.", 400)
    if parsed.scheme == "http" and parsed.hostname not in _LOOPBACK:
        raise AdoorError("INVALID_URL", "Remote servers require HTTPS.", 400)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"


def quote_ident(name: str) -> str:
    """A table or column name as a quoted SQL identifier."""
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError("Identifiers are nonempty strings.")
    return '"' + name.replace('"', '""') + '"'


class Result:
    """Rows from one statement. Iterate it, index it, or call ``df()`` for a pandas DataFrame."""

    def __init__(self, raw: Dict[str, Any], as_decimal: bool = False) -> None:
        self.raw = raw
        self.columns: List[Dict[str, Any]] = list(raw.get("columns") or [])
        self.rows: List[Dict[str, Any]] = decode_rows(list(raw.get("rows") or []), self.columns, as_decimal)
        self.row_count: int = int(raw.get("rowCount") or 0)
        self.command: Optional[str] = raw.get("command")
        self.event_id: Optional[str] = raw.get("eventId")
        self.summary: Optional[str] = raw.get("summary")
        self.replayed: bool = raw.get("replayed") is True
        self.truncated: bool = raw.get("truncated") is True
        self.next_cursor: Optional[str] = raw.get("nextCursor")

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index: int) -> Dict[str, Any]:
        return self.rows[index]

    def __repr__(self) -> str:
        return f"<Result {self.command} rows={len(self.rows)} rowCount={self.row_count}>"

    def df(self) -> Any:
        """The rows as a pandas DataFrame, in the result's column order. Needs ``pip install adoor[pandas]``."""
        try:
            import pandas as pd  # type: ignore[import-not-found]
        except ImportError as error:  # pragma: no cover - depends on the environment
            raise ImportError("Result.df() needs pandas: pip install 'adoor[pandas]'") from error
        names = [column["name"] for column in self.columns]
        return pd.DataFrame.from_records(self.rows, columns=names or None)

    to_pandas = df


class Sandbox:
    """A child identity minted for one sandbox, run or eval worker. The token is returned once."""

    def __init__(self, raw: Dict[str, Any], url: str) -> None:
        self.raw = raw
        self.id: str = raw["id"]
        self.name: str = raw.get("name", "")
        self.token: Optional[str] = raw.get("token")
        self.expires_at: Optional[str] = raw.get("effectiveExpiresAt") or raw.get("expiresAt")
        self.url = url

    def env(self) -> Dict[str, str]:
        """Environment variables for the sandbox process: ``ADOOR_URL`` and ``ADOOR_TOKEN``."""
        if not self.token:
            raise AdoorError("TOKEN_ALREADY_ISSUED", "This sandbox's token was returned by an earlier call.", 409)
        return {"ADOOR_URL": self.url, "ADOOR_TOKEN": self.token}

    def client(self, **options: Any) -> "Client":
        if not self.token:
            raise AdoorError("TOKEN_ALREADY_ISSUED", "This sandbox's token was returned by an earlier call.", 409)
        return Client(self.url, self.token, **options)

    def __repr__(self) -> str:
        return f"<Sandbox {self.name} {self.id[:8]} expires {self.expires_at}>"


class Client:
    """One identity talking to one ADoor server.

    ``space`` and ``workspace`` are defaults for every data call; pass ``space=`` on a call to override.
    """

    def __init__(self, url: str = DEFAULT_URL, token: TokenSource = "", *, space: Optional[str] = None,
                 workspace: Optional[str] = None, timeout: float = 60.0, retries: int = 3, busy_timeout: float = 120.0,
                 as_decimal: bool = False, headers: Optional[Mapping[str, str]] = None) -> None:
        if not token:
            raise AdoorError("TOKEN_REQUIRED", "Pass a token, set ADOOR_TOKEN, or sign in with adoor.login().", 401)
        self.url = _origin(url)
        self._token = token
        self.space = space
        self.workspace = workspace
        self.timeout = timeout
        self.retries = max(0, int(retries))
        self.busy_timeout = max(0.0, float(busy_timeout))
        self.as_decimal = as_decimal
        self._headers = dict(headers or {})
        self._opener = urllib.request.build_opener(_NoRedirect())

    def __repr__(self) -> str:
        return f"<adoor.Client {self.url}>"

    # Transport

    def _request(self, method: str, path: str, body: Any = None, timeout: Optional[float] = None) -> Any:
        if not path.startswith("/") or path.startswith("//") or "\\" in path:
            raise AdoorError("INVALID_PATH", "Authenticated requests must stay on the configured server.", 400)
        token = self._token() if callable(self._token) else self._token
        headers = {**self._headers, "Authorization": f"Bearer {token}", "Accept": "application/json",
                   "User-Agent": f"adoor-python/{VERSION}"}
        data = None
        if body is not None:
            data = json.dumps(body, allow_nan=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.url + path, data=data, method=method, headers=headers)
        try:
            with self._opener.open(request, timeout=self.timeout if timeout is None else timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            with error:
                failure = error_from_body(error.code, error.read())
                header = error.headers.get("Retry-After") if error.headers else None
            if failure.retry_after is None and header and header.strip().isdigit():
                failure.retry_after = float(header.strip())
            raise failure from None
        except (urllib.error.URLError, OSError) as error:
            reason = getattr(error, "reason", error)
            raise AdoorError("NETWORK", f"The request could not reach {self.url}: {reason}", 0) from error
        return json.loads(raw.decode("utf-8")) if raw else None

    def _with_retries(self, send: Callable[[], Any], safe: bool) -> Any:
        """Retry a refusal that ran nothing until ``busy_timeout``; retry other transient failures only when safe."""
        attempt, busy = 0, 0
        deadline = time.monotonic() + self.busy_timeout
        while True:
            try:
                return send()
            except AdoorError as error:
                if error.code in _NOT_STARTED and time.monotonic() < deadline:
                    base = error.retry_after if error.retry_after is not None else 0.25 * 2**min(busy, 5)
                    time.sleep(min(30.0, max(0.05, base)) * (0.5 + random.random()))
                    busy += 1
                    continue
                if not safe or not error.retryable or attempt >= self.retries:
                    raise
                delay = error.retry_after if error.retry_after is not None else 0.5 * 2**attempt
                time.sleep(min(30.0, max(0.05, delay)))
                attempt += 1

    def call(self, operation: str, input: Optional[Mapping[str, Any]] = None, *, safe_to_retry: bool = False,
             timeout: Optional[float] = None) -> Any:
        """Run one named operation, such as ``data.query``. ``GET {url}/api/operations`` lists them."""
        if not _OPERATION.match(operation):
            raise AdoorError("INVALID_OPERATION", "Operation names look like data.query.", 400)
        body = dict(input or {})
        return self._with_retries(lambda: self._request("POST", f"/api/operations/{operation}", body, timeout), safe_to_retry)

    def _target(self, body: Dict[str, Any], space: Optional[str], workspace: Optional[str]) -> Dict[str, Any]:
        chosen_space = space if space is not None else self.space
        chosen_workspace = workspace if workspace is not None else self.workspace
        if chosen_space:
            body["spaceId"] = chosen_space
        if chosen_workspace:
            body["workspace"] = chosen_workspace
        return body

    # SQL

    def _sql(self, operation: str, sql: str, params: Optional[Sequence[Any]], *, space: Optional[str],
             workspace: Optional[str], idempotency_key: Optional[str], expect_row_count: Optional[int],
             limit: Optional[int], cursor: Optional[str], atomic: Optional[bool], dry_run: bool,
             timeout: Optional[float] = None) -> Result:
        base: Dict[str, Any] = {"sql": sql}
        if params is not None:
            base["params"] = [encode_param(value) for value in params]
        self._target(base, space, workspace)
        if expect_row_count is not None:
            base["expectRowCount"] = expect_row_count
        if limit is not None:
            base["limit"] = limit
        if atomic is not None:
            base["atomic"] = atomic
        if dry_run:
            base["dryRun"] = True
        safe = operation == "data.query" or idempotency_key is not None or dry_run

        def page(next_cursor: Optional[str]) -> Dict[str, Any]:
            body = dict(base)
            if next_cursor is not None:
                body["cursor"] = next_cursor
            elif cursor is not None:
                body["cursor"] = cursor
            elif idempotency_key is not None:
                body["idempotencyKey"] = idempotency_key
            return self.call(operation, body, safe_to_retry=safe or next_cursor is not None, timeout=timeout)

        first = page(None)
        if not isinstance(first, dict) or "rows" not in first or first.get("batch"):
            return Result(first if isinstance(first, dict) else {"rows": []}, self.as_decimal)
        if limit is not None or cursor is not None or first.get("truncated") is not True:
            return Result(first, self.as_decimal)
        rows = list(first.get("rows") or [])
        seen = set()
        current = first
        while current.get("truncated") is True and isinstance(current.get("nextCursor"), str) \
                and current["nextCursor"] not in seen:
            seen.add(current["nextCursor"])
            current = page(current["nextCursor"])
            rows.extend(current.get("rows") or [])
        merged = {**current, "rows": rows, "rowCount": len(rows), "columns": first.get("columns")}
        return Result(merged, self.as_decimal)

    def query(self, sql: str, params: Optional[Sequence[Any]] = None, *, space: Optional[str] = None,
              workspace: Optional[str] = None, limit: Optional[int] = None, cursor: Optional[str] = None) -> Result:
        """Run SELECT statements. Every page is followed unless you pass ``limit`` or ``cursor``."""
        return self._sql("data.query", sql, params, space=space, workspace=workspace, idempotency_key=None,
                         expect_row_count=None, limit=limit, cursor=cursor, atomic=None, dry_run=False)

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None, *, space: Optional[str] = None,
                workspace: Optional[str] = None, idempotency_key: Optional[str] = None,
                expect_row_count: Optional[int] = None, atomic: Optional[bool] = None,
                dry_run: bool = False, timeout: Optional[float] = None) -> Result:
        """Run SQL that changes data or schema. With ``idempotency_key`` a lost response is retried safely."""
        return self._sql("data.execute", sql, params, space=space, workspace=workspace,
                         idempotency_key=idempotency_key, expect_row_count=expect_row_count, limit=None,
                         cursor=None, atomic=atomic, dry_run=dry_run, timeout=timeout)

    def df(self, sql: str, params: Optional[Sequence[Any]] = None, **options: Any) -> Any:
        """``query(...).df()``: a SELECT as a pandas DataFrame."""
        return self.query(sql, params, **options).df()

    def insert(self, table: str, rows: Any, *, space: Optional[str] = None, workspace: Optional[str] = None,
               chunk_rows: Optional[int] = None, key: Optional[str] = None) -> Dict[str, Any]:
        """Insert dicts, or the rows of a pandas DataFrame, with multi-row INSERTs.

        A NaN cell is missing data and is stored as NULL, as ``pandas.DataFrame.to_sql`` does, so SQL
        aggregates such as ``avg`` and ``count`` skip it the way pandas does. To store PostgreSQL's
        ``'NaN'`` number, pass ``float("nan")`` as an ``execute`` parameter.

        Each chunk is one statement and one history entry, sent with its own idempotency key so a lost
        response is retried without inserting twice. ``key`` makes the keys stable across reruns of a
        notebook cell: rerunning with the same key and the same rows replays the first result.
        """
        records = _records(rows)
        if not records:
            return {"rowCount": 0, "eventIds": [], "chunks": 0}
        names: List[str] = []
        for record in records:
            for name in record:
                if name not in names:
                    names.append(name)
        prefix = f"INSERT INTO {quote_ident(table)} ({', '.join(quote_ident(name) for name in names)}) VALUES "
        limit_rows = max(1, min(_MAX_PARAMETERS // len(names), int(chunk_rows or _CHUNK_ROWS)))
        total, event_ids, chunk = 0, [], 0
        start = 0
        while start < len(records):
            params: List[Any] = []
            groups: List[str] = []
            size = 0
            for record in records[start:start + limit_rows]:
                values = [encode_cell(record.get(name)) for name in names]
                cost = sum(len(value) if isinstance(value, str) else 8 for value in values)
                if groups and size + cost > _CHUNK_BYTES:
                    break
                base = len(params)
                groups.append("(" + ", ".join(f"${base + index + 1}" for index in range(len(names))) + ")")
                params.extend(values)
                size += cost
            idempotency = f"{key}:{chunk}" if key else str(uuid.uuid4())
            result = self.execute(prefix + ", ".join(groups), params, space=space, workspace=workspace,
                                  idempotency_key=idempotency)
            total += result.row_count
            if result.event_id:
                event_ids.append(result.event_id)
            start += len(groups)
            chunk += 1
        return {"rowCount": total, "eventIds": event_ids, "chunks": chunk}

    def import_csv(self, table: str, source: Any, **options: Any) -> Dict[str, Any]:
        """Load a large CSV file (path, bytes or binary stream) with ``COPY FROM``. See ``adoor.bulk.import_csv``."""
        from .bulk import import_csv

        return import_csv(self, table, source, **options)

    def import_frame(self, table: str, rows: Any, **options: Any) -> Dict[str, Any]:
        """Load a large DataFrame or list of dicts through a temporary CSV and ``COPY FROM``. See ``adoor.bulk``."""
        from .bulk import import_frame

        return import_frame(self, table, rows, **options)

    def logger(self, table: str, *, flush_rows: int = 500, flush_seconds: float = 5.0,
               space: Optional[str] = None, workspace: Optional[str] = None, **fixed: Any) -> "Logger":
        """Buffer rows from a loop and write them in batches: one request per flush, not one per row.

        ``fixed`` columns, such as ``run="r42"``, are added to every row. Use it as a context manager, or
        call ``close()``, so the last rows are written. Each credential may send 300 requests a minute.
        """
        return Logger(self, table, flush_rows=flush_rows, flush_seconds=flush_seconds, space=space,
                      workspace=workspace, fixed=fixed)

    def create_table(self, table: str, columns: Any, *, primary_key: Optional[str] = "id",
                     space: Optional[str] = None, workspace: Optional[str] = None) -> Result:
        """Create a table from ``{"name": "sql type"}`` or from a DataFrame's dtypes.

        ``primary_key="id"`` adds ``id uuid PRIMARY KEY DEFAULT gen_random_uuid()`` unless a column has
        that name. Pass ``primary_key=None`` to add none.
        """
        spec = _column_types(columns)
        parts = []
        if primary_key and primary_key not in spec:
            parts.append(f"{quote_ident(primary_key)} uuid PRIMARY KEY DEFAULT gen_random_uuid()")
        for name, sql_type in spec.items():
            suffix = " PRIMARY KEY" if primary_key and name == primary_key else ""
            parts.append(f"{quote_ident(name)} {sql_type}{suffix}")
        return self.execute(f"CREATE TABLE {quote_ident(table)} ({', '.join(parts)})", space=space,
                            workspace=workspace)

    def tables(self, *, workspace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Tables this identity can see, with their space, columns and permissions."""
        body = self._target({}, None, workspace)
        return list((self.call("store.describe", body, safe_to_retry=True) or {}).get("tables") or [])

    # Identity and provenance

    def whoami(self) -> Dict[str, Any]:
        """This credential's identity: connection, workspace, permissions and expiry."""
        return self._with_retries(lambda: self._request("GET", "/api/identity"), True)

    def context(self, *, workspace: Optional[str] = None) -> Dict[str, Any]:
        return self.call("context.get", self._target({}, None, workspace), safe_to_retry=True)

    def history(self, *, table: Optional[str] = None, limit: int = 100, before: Optional[str] = None,
                space: Optional[str] = None, workspace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Activity, newest first: what changed, which identity changed it (``actorLabel``) and when.

        An identity lists its own changes. The workspace owner, and an identity with the ``recover``
        capability, also list every other identity's changes, such as the sandboxes it minted.
        """
        events: List[Dict[str, Any]] = []
        cursor = before
        while len(events) < limit:
            body: Dict[str, Any] = {"limit": min(100, limit - len(events))}
            if table:
                body["table"] = table
            if cursor:
                body["before"] = cursor
            page = self.call("history.read", self._target(body, space, workspace), safe_to_retry=True) or {}
            events.extend(page.get("activity") or [])
            cursor = page.get("nextCursor")
            if not page.get("truncated") or not cursor:
                break
        return events[:limit]

    def history_df(self, **options: Any) -> Any:
        """``history()`` as a DataFrame with ``createdAt`` parsed, one row per change."""
        import pandas as pd  # type: ignore[import-not-found]

        frame = pd.DataFrame.from_records(self.history(**options))
        if not frame.empty:
            frame["createdAt"] = pd.to_datetime(frame["createdAt"], utc=True)
            frame["table"] = frame["details"].map(lambda details: (details or {}).get("table"))
        return frame

    def inspect(self, event_id: str, *, workspace: Optional[str] = None) -> Dict[str, Any]:
        """One change with its row images: ``changes`` holds each row's ``before`` and ``after``."""
        body: Dict[str, Any] = {"id": str(event_id)}
        if workspace or self.workspace:
            body["workspace"] = workspace or self.workspace
        return self.call("history.read", body, safe_to_retry=True)

    def undo(self, event_id: str, *, workspace: Optional[str] = None) -> Dict[str, Any]:
        """Reverse one change. It stops, changing nothing, if a later change touched the same rows."""
        body: Dict[str, Any] = {"id": str(event_id)}
        if workspace or self.workspace:
            body["workspace"] = workspace or self.workspace
        return self.call("history.undo", body)

    # Child identities

    def sandboxes(self, count: Optional[int] = None, *, names: Optional[Sequence[str]] = None,
                  permissions: Optional[Sequence[Mapping[str, Any]]] = None, expires_in: int = 3600,
                  prefix: str = "sandbox", role: str = "sandbox", capabilities: Sequence[str] = (),
                  idempotency_key: Optional[str] = None) -> List[Sandbox]:
        """Mint one child identity per sandbox, run or eval worker in one request.

        Each child is capped by this identity's own permissions and expiry, writes under its own name in
        history, and can be revoked alone. Omitted ``permissions`` grant nothing. Needs the
        ``manage_connections`` capability. At most 1,000 per call.
        """
        labels = list(names) if names is not None else [f"{prefix}-{index + 1}" for index in range(int(count or 0))]
        if not labels:
            raise ValueError("Pass count or names.")
        grant = [dict(item) for item in (permissions or [])]
        items = [{"name": label, "role": role, "permissions": grant, "capabilities": list(capabilities),
                  "expiresIn": int(expires_in)} for label in labels]
        body: Dict[str, Any] = {"items": items, "idempotencyKey": idempotency_key or str(uuid.uuid4())}
        response = self.call("connections.create", body, safe_to_retry=True) or {}
        return [Sandbox(item, self.url) for item in response.get("connections") or []]

    def revoke(self, connection_id: str) -> Dict[str, Any]:
        """Revoke a child identity. Its next request fails, and so do its own children's."""
        return self.call("connections.revoke", {"id": connection_id})

    def connections(self, *, parent: Optional[str] = None, role: Optional[str] = None,
                    limit: int = 100) -> List[Dict[str, Any]]:
        body: Dict[str, Any] = {"limit": limit}
        if parent:
            body["parentId"] = parent
        if role:
            body["role"] = role
        response = self.call("connections.list", body, safe_to_retry=True) or {}
        return list(response.get("connections") or response.get("items") or [])


class Logger:
    """Rows buffered for one table. ``log(**row)`` queues; a full buffer or ``flush_seconds`` writes them."""

    def __init__(self, client: Client, table: str, *, flush_rows: int, flush_seconds: float,
                 space: Optional[str], workspace: Optional[str], fixed: Mapping[str, Any]) -> None:
        self.client = client
        self.table = table
        self.flush_rows = max(1, int(flush_rows))
        self.flush_seconds = float(flush_seconds)
        self.space = space
        self.workspace = workspace
        self.fixed = dict(fixed)
        self.rows: List[Dict[str, Any]] = []
        self.written = 0
        self.event_ids: List[str] = []
        self._key = str(uuid.uuid4())
        self._flushes = 0
        self._last = time.monotonic()

    def log(self, **row: Any) -> None:
        self.rows.append({**self.fixed, **row})
        if len(self.rows) >= self.flush_rows or time.monotonic() - self._last >= self.flush_seconds:
            self.flush()

    def flush(self) -> None:
        self._last = time.monotonic()
        if not self.rows:
            return
        pending, self.rows = self.rows, []
        try:
            result = self.client.insert(self.table, pending, space=self.space, workspace=self.workspace,
                                        key=f"{self._key}:{self._flushes}")
        except Exception:
            self.rows = pending + self.rows
            raise
        self._flushes += 1
        self.written += result["rowCount"]
        self.event_ids.extend(result["eventIds"])

    def close(self) -> None:
        self.flush()

    def __enter__(self) -> "Logger":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.flush()


def _records(rows: Any) -> List[Dict[str, Any]]:
    to_dict = getattr(rows, "to_dict", None)
    if callable(to_dict) and hasattr(rows, "columns"):
        return [dict(record) for record in to_dict(orient="records")]
    if isinstance(rows, Mapping):
        return [dict(rows)]
    return [dict(record) for record in rows]


_DTYPE_TYPES = (("bool", "boolean"), ("int", "bigint"), ("uint", "bigint"), ("float", "numeric"),
                ("datetime64", "timestamptz"), ("object", "text"), ("string", "text"), ("category", "text"))


def _column_types(columns: Any) -> Dict[str, str]:
    dtypes = getattr(columns, "dtypes", None)
    if dtypes is None:
        return {str(name): str(sql_type) for name, sql_type in dict(columns).items()}
    spec: Dict[str, str] = {}
    for name, dtype in dtypes.items():
        kind = str(dtype).lower()
        spec[str(name)] = next((sql for prefix, sql in _DTYPE_TYPES if kind.startswith(prefix)), "text")
    return spec


def connect(url: Optional[str] = None, token: Optional[TokenSource] = None, **options: Any) -> Client:
    """A client from arguments or the environment: ``ADOOR_URL`` (default https://app.adoor.run) and ``ADOOR_TOKEN``."""
    chosen_url = url or os.environ.get("ADOOR_URL") or DEFAULT_URL
    chosen_token = token or os.environ.get("ADOOR_TOKEN") or os.environ.get("ADOOR_KEY")
    if not chosen_token:
        raise AdoorError("TOKEN_REQUIRED", "Set ADOOR_TOKEN, pass token=, or sign in with adoor.login().", 401)
    return Client(chosen_url, chosen_token, **options)

