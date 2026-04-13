# dxtx

SQD file-store indexer for ERC20 transfers across the token universe from `dx-token-list`.

## Layout

- `token-list/`: vendored `dx-token-list` snapshot used by default in cloud and local runs
- `src/<chain>/publish.ts`: one long-running publisher entrypoint per chain
- `src/all/publish.ts`: launches all selected chain publishers in parallel
- `src/publish/run.ts`: chain setup, resume logic, metadata resolution, and processor launch
- `src/publish/metadata.ts`: Dune-first token metadata warmup with RPC fallback
- `src/publish/manifest.ts`: publishes `token-manifest.parquet` alongside transfer chunks
- `src/processor-cache.ts`: SQD `EvmBatchProcessor` + file-store parquet writer

## Commands

Build once:

```bash
npm run build
```

Run all chains with the SQD command surface:

```bash
CACHE_FROM_DATE=2026-01-01 sqd process:all
```

Run one chain:

```bash
CACHE_FROM_DATE=2026-01-01 sqd process:base
```

Check that transfer data is landing in the cache:

```bash
npm run check:dataflow -- base
```

Check block freshness against the current SQD archive height:

```bash
npm run check:freshness -- base
```

Build or refresh a local analytics database for one chain:

```bash
npm run build:flows -- ethereum
```

Incrementally sync flows from newly published chunks into a dedicated local DuckDB:

```bash
npm run sync:flows -- ethereum
```

Start the local serving Postgres used by `sync:flows`:

```bash
npm run db:up
```

Run the flow sync worker continuously:

```bash
FLOW_SYNC_LOOP=1 FLOW_SYNC_INTERVAL_SEC=300 npm run sync:flows -- ethereum
```

Run flow sync across the selected chains in parallel:

```bash
CACHE_CHAINS=ethereum,base,arbitrum FLOW_SYNC_LOOP=1 FLOW_SYNC_INTERVAL_SEC=300 npm run sync:flows
```

Inspect a token flow slice from the local analytics database:

```bash
npm run inspect:flows -- ethereum usdc
```

Verify flow rows against cached transfers and RPC receipts:

```bash
npm run verify:flows -- ethereum usdc 2026-01-31 0x...
```

If you are not using the `sqd` CLI, the built node entrypoints are:

```bash
node --require=dotenv/config lib/all/publish.js
node --require=dotenv/config lib/base/publish.js
```

`process:all` resolves Dune token metadata once for the selected token universe, then shares that snapshot with the per-chain publishers. For open-ended runs without `CACHE_TO_BLOCK*`, it keeps polling after catch-up instead of exiting. Chain publishers only use RPC for misses.

Run a bounded dev slice:

```bash
CACHE_FROM_DATE=2026-01-01 \
CACHE_CHAINS=base,ethereum \
CACHE_TARGET_LIMIT=5 \
CACHE_TO_BLOCK_BASE=40218324 \
CACHE_TO_BLOCK_ETHEREUM=24336350 \
sqd process:all
```

## Required env

- `CACHE_DEST`
  local path or `s3://bucket/prefix`
- `CACHE_FROM_DATE` or `CACHE_FROM_BLOCK`
  starting point for the backfill
- `CACHE_S3_ENDPOINT`, `CACHE_S3_REGION`, `CACHE_S3_ACCESS_KEY_ID`, `CACHE_S3_SECRET_ACCESS_KEY`
  required when `CACHE_DEST` points at B2/S3

## Optional env

- `CACHE_TO_BLOCK`
  stop block for a bounded run
- `CACHE_POLL_INTERVAL_SEC`
  sleep interval between open-ended `process:all` polling passes after catch-up; defaults to `60`
- `CACHE_CHUNK_SIZE_MB`
  target SQD file-store chunk size in MB
- `CACHE_LOG_FILTER_MODE`
  `address` to request only target-token logs from SQD, or `topic0` to request all ERC20 `Transfer` logs and filter client-side
- `CACHE_CHAINS`
  comma-separated chain list for `process:all`
- `CACHE_TARGET_LIMIT`
  dev-only cap on how many token targets to index per chain
- `DX_TOKEN_LIST_ROOT`
  override the bundled token list snapshot path
- `DUNE_API_KEY`
  optional token metadata source; falls back to RPC when missing or rate-limited
- `FLOW_SYNC_DB_PATH`
  local DuckDB path for the incremental flow sync worker; defaults to `./analytics/<chain>-flow-sync.duckdb`
- `FLOW_SYNC_MAX_CHUNKS`
  max number of new cache chunks to process per sync pass; defaults to `32`
- `ANALYTICS_MEMORY_LIMIT`
  DuckDB memory cap for flow sync workers; defaults to `4GB`
- `ANALYTICS_THREADS`
  DuckDB thread count for flow sync workers
- `FLOW_SYNC_LOOP`
  when `true`, keep polling for new chunks instead of running a single sync pass
- `FLOW_SYNC_INTERVAL_SEC`
  sleep interval between polling passes when `FLOW_SYNC_LOOP=true`; defaults to `300`
- `FLOW_SYNC_CONCURRENCY`
  one-shot worker concurrency when `FLOW_SYNC_LOOP` is not set; defaults to `2`
- `FLOW_SYNC_RESET`
  when `true`, drop the local incremental flow-sync state before the next pass
- `FLOW_SYNC_FROM_DAY`, `FLOW_SYNC_TO_DAY`
  optional day-bounded backfill range for the incremental flow sync worker
- `FLOWS_DATABASE_URL`
  optional explicit Postgres connection string; when set, `sync:flows` publishes compact serving tables after each pass
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME`
  optional local Docker Postgres settings; if `FLOWS_DATABASE_URL` is unset and any `DB_*` value is set, `sync:flows` derives a local Postgres connection string from these values

Per-chain overrides use the same suffix pattern:

- `CACHE_FROM_DATE_BASE`
- `CACHE_FROM_BLOCK_ETHEREUM`
- `CACHE_TO_BLOCK_ARBITRUM`
- `CACHE_CHUNK_SIZE_MB_BASE`
- `CACHE_TARGET_LIMIT_BASE`
- `FLOW_SYNC_MAX_CHUNKS_BSC`
- `ANALYTICS_MEMORY_LIMIT_BSC`
- `ANALYTICS_THREADS_BSC`

## Output

Each chain publishes under its own cache prefix:

- `<cache-dest>/<chain>/token-manifest.parquet`
- `<cache-dest>/<chain>/erc20-transfers/<chunk-range>/transfers.parquet`
- `<cache-dest>/<chain>/erc20-transfers/status.txt`

Resume is cache-first. The publisher resumes from the last flushed transfer chunk in the file-store output.

## Production shape

- Keep the raw transfer cache publishing continuously to B2 with `sqd process:*`.
- Run `sync:flows` against a separate local DuckDB database on the indexer box.
- Run a local Dockerized Postgres for serving tables with `npm run db:up`.
- Let `npm run sync:flows` fan out to one worker per selected chain in loop mode, with each chain keeping its own local DuckDB file.
- Let `sync:flows` process only new parquet chunks and maintain:
  - `processed_flow_chunks`
  - `token_daily_totals`
  - `token_daily_address_flows`
  - `token_daily_top_flows`
  - `token_flow_leaderboards_current`
- When `FLOWS_DATABASE_URL` is set, or when local `DB_*` settings are present, publish compact serving tables to Postgres:
  - `token_flow_daily_totals`
  - `token_flow_leaderboards`
  - these serving tables use `network` values aligned with Dexosphere, such as `eth`, `base`, `arbitrum`, and `bsc`

`build:flows` is still useful for ad hoc or bounded historical analysis. `sync:flows` is the long-running production path and uses its own local DuckDB file by default so it does not conflict with the ad hoc analytics database.

For a local serving database, the default Docker/Postgres shape is:

```bash
DB_PORT=15432
DB_USER=squid
DB_PASS=squid
DB_NAME=dxtx_flows
```

With those values in `.env`, `npm run db:up` starts Postgres and `npm run sync:flows -- ethereum` will publish serving tables there without needing a separate `FLOWS_DATABASE_URL`.
