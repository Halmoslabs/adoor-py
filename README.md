# adoor

ADoor for Python. Issues are welcome on [Halmoslabs/adoor-py](https://github.com/Halmoslabs/adoor-py/issues). The code is developed in a private repository and mirrored here on each release. It is [MIT licensed](LICENSE). Copyright Halmos Labs.

ADoor is a shared PostgreSQL database for a team, its scripts and its agents. Each notebook, pipeline or sandbox connects as its own identity with only the access you give it, and every insert, update and delete is recorded with which identity made it and when. It needs Python 3.9 or newer and has no required dependencies; `pip install "adoor[pandas]"` adds DataFrames.

```sh
pip install "adoor[pandas]"
```

## Sign in from a notebook

```python
import adoor

db = adoor.login(name="Growth curves notebook")
```

ADoor opens in your browser and you choose what this connection can reach. The connection is named after `name`, and every change it makes carries that name in Activity. The credential is saved in `~/.config/adoor/credentials.json` (file mode 0600) and reused when you rerun the cell. Pass `save=False` to keep it in memory only.

For a script or a job, create a connection in ADoor (Access, New connection), then set `ADOOR_TOKEN` and call `adoor.connect()`. `ADOOR_URL` defaults to `https://app.adoor.run`.

## Write and read

```python
db.create_table("plate_reads", {"plate": "text", "well": "text", "od600": "numeric", "read_at": "timestamptz"})
db.insert("plate_reads", frame)          # a DataFrame, or a list of dicts
db.df("SELECT well, avg(od600) AS od600 FROM plate_reads WHERE plate = $1 GROUP BY well ORDER BY well", ["P1"])
```

- In `insert`, a NaN cell (pandas' missing value) is stored as NULL, as `DataFrame.to_sql` does, so `avg` and `count` in SQL skip it like pandas does.
- `insert` sends multi-row INSERTs of 1,000 rows (`chunk_rows=`), at most 65,535 parameters and about 4 MB each. Each chunk is one change in history and carries its own idempotency key, so a lost response is retried without inserting twice. Pass `key="plate-P1"` and rerunning the same cell with the same rows replays the first result instead of inserting again.
- `create_table` also takes a DataFrame and maps its dtypes: integers to `bigint`, floats to `numeric`, booleans, aware datetimes to `timestamptz`, the rest to `text`. It adds `id uuid PRIMARY KEY DEFAULT gen_random_uuid()` unless you pass `primary_key=None`.
- `query` returns a `Result`: iterate it, index it, or call `.df()`. Every page is followed. `bigint` comes back as `int`, `numeric` as `float` (or `Decimal` with `adoor.connect(as_decimal=True)`), `timestamptz` as an aware `datetime`, `date` as `date`, and `jsonb` as dicts and lists.
- Parameters are `$1, $2, …`. Python values are converted: `datetime` needs a time zone, a `NaN` parameter is PostgreSQL's `NaN` number, infinities go to a `numeric` column, lists become PostgreSQL arrays, and dicts (or `adoor.Json(value)`) become JSON.
- `execute(sql, params, expect_row_count=1)` refuses with `CONFLICT` and changes nothing when a guarded `UPDATE` matches a different number of rows. `dry_run=True` rolls the statement back.
- ADoor's column types are `text`, `integer`, `bigint`, `numeric`, `boolean`, `timestamptz`, `date`, `uuid`, `json`, `jsonb`, and one-dimensional arrays of those. There is no `double precision`; use `numeric`. NaN inside a `numeric[]` reads back as `None`; NaN in a `numeric` column reads back as NaN.

## Load a large file or DataFrame

```python
db.import_csv("plate_reads", "reads.csv")                 # a path, bytes, or a binary stream
db.import_frame("plate_reads", frame, create=True)        # a DataFrame or a list of dicts
```

- `import_csv` uploads the file once and runs `COPY plate_reads (…) FROM 'adoor://…' WITH (FORMAT csv, HEADER true)`. The header names the columns, in any order; pass `columns=[…]` and `header=False` for a file without one. The server reads the upload in chunks of 200 rows or 1 MiB and commits each chunk as one change in history, so neither the request nor the client holds the whole file.
- If the connection drops or no response arrives within `request_timeout` (900 s by default), the client sends the same `COPY` with the same idempotency key, and the server continues after the last committed chunk, up to `attempts` times (10). Pass `key=` to resume a load from a new process.
- `import_frame` writes the rows to a temporary CSV file and calls `import_csv`. As in `insert`, NaN and `None` become NULL; strings are quoted, so an empty string stays an empty string. `create=True` creates the table from the DataFrame's dtypes first.
- Both return `rowCount` (rows committed), `attempts` (COPY statements sent), `bytes`, the file's `sha256`, `uploadSeconds` and `copySeconds`.
- Choosing: on one machine a million rows took 389 s with `insert` and 467 s with `import_frame`, but `insert` held 942 MB in the client against 241 MB for the DataFrame alone, and sent 1,000 requests instead of one upload and a COPY. Use `import_csv` for a file on disk and `import_frame` when memory or one resumable load matters; `insert` is fine for anything that fits comfortably in memory. Loads count toward the workspace's database size like any other rows.

## Log a run

```python
with db.logger("metrics", run="lr-3e-4", flush_rows=500, flush_seconds=5) as log:
    for step, batch in enumerate(loader):
        loss = train_step(batch)
        log.log(step=step, loss=float(loss))
```

The logger buffers rows and writes them as one insert per flush, when `flush_rows` rows are waiting or `flush_seconds` have passed since the last write, and once more on exit. Keyword arguments to `logger` are added to every row. Each credential may send 300 requests a minute, so write a loop's rows in batches like this rather than one request per step.

## Who changed what

```python
db.history_df(table="plate_reads")       # one row per change: event, connectionName, actorLabel, createdAt
db.inspect(event_id)["changes"]          # each row's before and after
db.undo(event_id)                        # stops, changing nothing, if a later change touched the same rows
db.whoami()                              # this credential: connection, workspace, permissions, expiry
```

An identity lists its own changes. The workspace owner, and a connection with the `recover` capability, list everyone's. Activity keeps who made each change and when. Row images, the before and after values that `inspect` and `undo` use, are kept for the last 5,000 row changes per workspace or 30 days, whichever is shorter. For a permanent record, store the values you need in your own tables, or export them.

## One identity per sandbox, run or eval worker

```python
boxes = db.sandboxes(50, prefix="eval", expires_in=3600,
                     permissions=[{"table": "eval_results", "actions": ["select", "insert"]}])
for box in boxes:
    launch_worker(env=box.env())         # ADOOR_URL and ADOOR_TOKEN for that worker
```

One request mints up to 1,000 children. Each child is capped by the caller's own permissions and expiry, writes under its own name in history, and can be revoked alone with `db.revoke(box.id)`; its next request fails. Omitted permissions grant nothing. Minting needs the `manage_connections` capability. Each token is returned once.

A CI job or a cloud sandbox with an OIDC token can skip stored secrets. A person approves a trust for that issuer and subject in ADoor, then the job calls:

```python
db = adoor.exchange(adoor.github_actions_token("https://app.adoor.run"), trust_id="…")
```

The identity it returns lasts at most 15 minutes and is capped by the trust.

## Errors

Every refusal is `adoor.AdoorError` with the server's `code`, HTTP `status` and `details`. `error.retryable` says whether the same request may succeed later. `error.authority` says the identity's access ended or changed. A refusal that ran nothing (`RATE_LIMITED`, `QUERY_BUSY` when the workspace's SQL queue is full, `QUEUE_FULL`) is retried after the server's `Retry-After`, with jitter, for up to `busy_timeout` seconds (default 120). Other transient failures are retried up to `retries` times (default 3) for reads, and for writes only when they carry an idempotency key.

`db.call("operation.name", {...})` runs any operation the server lists at `GET /api/operations`.
