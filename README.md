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

If you are not using the `sqd` CLI, the built node entrypoints are:

```bash
node --require=dotenv/config lib/all/publish.js
node --require=dotenv/config lib/base/publish.js
```

`process:all` resolves Dune token metadata once for the selected token universe, then shares that snapshot with the per-chain publishers. Chain publishers only use RPC for misses.

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

Per-chain overrides use the same suffix pattern:

- `CACHE_FROM_DATE_BASE`
- `CACHE_FROM_BLOCK_ETHEREUM`
- `CACHE_TO_BLOCK_ARBITRUM`
- `CACHE_CHUNK_SIZE_MB_BASE`
- `CACHE_TARGET_LIMIT_BASE`

## Output

Each chain publishes under its own cache prefix:

- `<cache-dest>/<chain>/token-manifest.parquet`
- `<cache-dest>/<chain>/erc20-transfers/<chunk-range>/transfers.parquet`
- `<cache-dest>/<chain>/erc20-transfers/status.txt`

Resume is cache-first. The publisher resumes from the last flushed transfer chunk in the file-store output.
