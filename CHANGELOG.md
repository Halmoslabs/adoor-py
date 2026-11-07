# Changelog

## 0.1.2

Fixes the release script so the first public release actually publishes. The package is unchanged from the unpublished 0.1.1.

## 0.1.1

First published release. The package is unchanged from the unpublished 0.1.0.

## 0.1.0

First release of ADoor for Python.

- `connect()` from `ADOOR_URL` and `ADOOR_TOKEN`, and `login()`: approve a notebook or script in the browser, with the credential saved at mode 0600 and reused.
- `query`, `execute`, `df` and `insert`: typed results, pandas DataFrames, every page followed, multi-row inserts with one idempotency key per chunk.
- `import_csv` and `import_frame`: load large files and DataFrames with `COPY FROM`, committed in chunks and resumed after a dropped connection.
- `logger`: buffer a loop's rows and write them in batches, one request per flush.
- `history`, `history_df`, `inspect` and `undo`: which identity changed what, and when, with row images.
- `sandboxes` and `revoke`: one child identity per sandbox or eval worker, capped by the caller. `exchange` trades a workload's OIDC token for a short-lived identity.
