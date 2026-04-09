import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { DuckDBInstance, type DuckDBConnection } from "@duckdb/node-api";
import { getCacheDest, getTokenManifestPath, isS3Uri, listTransferChunks } from "./cache.js";
import { resolveRpcUrl, resolveSqdUrl, supportedChains } from "./chains.js";
import { findBlockByTimestamp } from "./evm.js";
import { escapeSqlString } from "./format.js";
import { logLine } from "./log.js";
import type { Chain } from "./types.js";

const assertEnv = (value: string | undefined, key: string) => {
  if (!value) {
    throw new Error(`${key} is required when CACHE_DEST points at s3://...`);
  }

  return value;
};

const envValue = (key: string) => process.env[key];
const parseDay = (value: string | undefined, key: string) => {
  if (value === undefined) {
    return undefined;
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`invalid ${key}: ${value}; expected YYYY-MM-DD`);
  }

  return value;
};

const splitChainArgs = (values: string[]) =>
  values.flatMap((value) => value.split(",")).map((value) => value.trim()).filter(Boolean);

const isChain = (value: string): value is Chain => supportedChains.includes(value as Chain);

const selectedChain = (args: string[]): Chain => {
  const requested = splitChainArgs(args);

  if (requested.length === 0) {
    return "ethereum";
  }

  const chains = [...new Set(requested.filter(isChain))];

  if (chains.length === 0) {
    throw new Error(`no supported chains selected; expected one of ${supportedChains.join(",")}`);
  }

  if (chains.length > 1) {
    throw new Error(`build:flows currently supports one chain at a time; received ${chains.join(",")}`);
  }

  const [chain] = chains;

  if (!chain) {
    throw new Error("no chain selected");
  }

  return chain;
};

const createConnection = async (databasePath: string) => {
  const instance = await DuckDBInstance.create(databasePath);
  return instance.connect();
};

const run = async (connection: DuckDBConnection, sql: string) => {
  await connection.run(sql);
};

const rows = async <T>(connection: DuckDBConnection, sql: string) =>
  (await connection.runAndReadAll(sql)).getRowObjectsJS() as T[];

const configureS3 = async (connection: DuckDBConnection, env: NodeJS.ProcessEnv) => {
  const endpointValue = assertEnv(env.CACHE_S3_ENDPOINT, "CACHE_S3_ENDPOINT");
  const endpoint = endpointValue.replace(/^[a-z]+:\/\//i, "").replace(/\/+$/, "");
  const region = assertEnv(env.CACHE_S3_REGION, "CACHE_S3_REGION");
  const accessKeyId = assertEnv(env.CACHE_S3_ACCESS_KEY_ID, "CACHE_S3_ACCESS_KEY_ID");
  const secretAccessKey = assertEnv(env.CACHE_S3_SECRET_ACCESS_KEY, "CACHE_S3_SECRET_ACCESS_KEY");
  const sessionToken = env.CACHE_S3_SESSION_TOKEN;
  const useSsl = endpointValue.startsWith("http://") ? "false" : "true";

  await run(connection, "INSTALL httpfs; LOAD httpfs");
  await run(connection, `SET s3_region='${escapeSqlString(region)}'`);
  await run(connection, `SET s3_endpoint='${escapeSqlString(endpoint)}'`);
  await run(connection, `SET s3_access_key_id='${escapeSqlString(accessKeyId)}'`);
  await run(connection, `SET s3_secret_access_key='${escapeSqlString(secretAccessKey)}'`);
  await run(connection, `SET s3_url_style='path'`);
  await run(connection, `SET s3_use_ssl=${useSsl}`);

  if (sessionToken) {
    await run(connection, `SET s3_session_token='${escapeSqlString(sessionToken)}'`);
  }
};

const sqlStringList = (values: string[]) =>
  `[${values.map((value) => `'${escapeSqlString(value)}'`).join(", ")}]`;

const outputPath = (chain: Chain) =>
  process.env.ANALYTICS_DB_PATH || `./analytics/${chain}-flows.duckdb`;

const dayRange = () => {
  const fromDay = parseDay(envValue("ANALYTICS_FROM_DAY"), "ANALYTICS_FROM_DAY");
  const toDay = parseDay(envValue("ANALYTICS_TO_DAY"), "ANALYTICS_TO_DAY");

  if (fromDay && toDay && fromDay > toDay) {
    throw new Error(`invalid day range: ${fromDay} > ${toDay}`);
  }

  return { fromDay, toDay };
};

const nextDay = (value: string) => {
  const timestamp = Date.parse(`${value}T00:00:00Z`);
  return new Date(timestamp + 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
};

const dayTimestamp = (value: string) => Math.floor(Date.parse(`${value}T00:00:00Z`) / 1_000);

const tempDirectory = (chain: Chain) =>
  process.env.ANALYTICS_TEMP_DIRECTORY || `./analytics/${chain}-duckdb-tmp`;

const configureDuckDb = async ({
  connection,
  chain,
}: {
  connection: DuckDBConnection;
  chain: Chain;
}) => {
  const memoryLimit = envValue("ANALYTICS_MEMORY_LIMIT") || "4GB";
  const threads = envValue("ANALYTICS_THREADS");
  const tempDir = tempDirectory(chain);

  await mkdir(tempDir, { recursive: true });
  await run(connection, `SET memory_limit='${escapeSqlString(memoryLimit)}'`);
  await run(connection, `SET temp_directory='${escapeSqlString(tempDir)}'`);
  await run(connection, "SET preserve_insertion_order=false");

  if (threads) {
    await run(connection, `SET threads=${Number.parseInt(threads, 10)}`);
  }
};

const sourceViewSql = ({
  chain,
  chunkUris,
  fromDay,
  toDay,
}: {
  chain: Chain;
  chunkUris: string[];
  fromDay?: string;
  toDay?: string;
}) => `
  create or replace temp table source_transfers as
  select
    '${escapeSqlString(chain)}' as chain,
    cast(date_trunc('day', timezone('UTC', to_timestamp(block_timestamp))) as date) as day,
    token_address,
    transaction_hash,
    from_address,
    to_address,
    coalesce(amount_double, try_cast(amount_text as double)) as amount_native,
    block_number
  from read_parquet(${sqlStringList(chunkUris)})
  where true
    ${fromDay ? `and cast(date_trunc('day', timezone('UTC', to_timestamp(block_timestamp))) as date) >= date '${escapeSqlString(fromDay)}'` : ""}
    ${toDay ? `and cast(date_trunc('day', timezone('UTC', to_timestamp(block_timestamp))) as date) <= date '${escapeSqlString(toDay)}'` : ""}
`;

const manifestTableSql = ({
  manifestUri,
}: {
  manifestUri: string;
}) => `
  create or replace table token_manifest as
  select *
  from read_parquet('${escapeSqlString(manifestUri)}')
`;

const dailyTokenTotalsSelectSql = `
  select
    chain,
    day,
    token_address,
    count(*) as transfer_count,
    sum(amount_native) as amount_native_sum,
    avg(amount_native) as avg_amount_native
  from source_transfers
  where amount_native is not null
  group by 1, 2, 3
`;

const dailyTopFlowsSelectSql = `
  with senders as (
    select
      chain,
      day,
      token_address,
      'sender' as direction,
      from_address as address,
      count(*) as transfer_count,
      sum(amount_native) as amount_native_sum,
      avg(amount_native) as avg_amount_native
    from source_transfers
    where amount_native is not null
    group by 1, 2, 3, 4, 5
  ),
  recipients as (
    select
      chain,
      day,
      token_address,
      'recipient' as direction,
      to_address as address,
      count(*) as transfer_count,
      sum(amount_native) as amount_native_sum,
      avg(amount_native) as avg_amount_native
    from source_transfers
    where amount_native is not null
    group by 1, 2, 3, 4, 5
  )
  ,
  flows as (
    select *
    from senders
    union all
    select *
    from recipients
  ),
  ranked as (
    select
      flows.*,
      row_number() over (
        partition by chain, day, token_address, direction
        order by amount_native_sum desc, transfer_count desc, address asc
      ) as flow_rank
    from flows
  )
  select
    ranked.chain,
    ranked.day,
    ranked.token_address,
    manifest.token_name,
    manifest.token_symbol,
    manifest.token_decimals,
    manifest.target_source,
    manifest.coingecko_id,
    manifest.coingecko_name,
    manifest.coingecko_symbol,
    ranked.direction,
    ranked.address,
    ranked.flow_rank,
    ranked.transfer_count,
    ranked.amount_native_sum,
    ranked.avg_amount_native
  from ranked
  left join token_manifest as manifest
    on manifest.chain = ranked.chain
   and manifest.address = ranked.token_address
  where ranked.flow_rank <= 50
`;

const summarySql = `
  select
    count(*) as leaderboard_rows,
    count(distinct token_address) as tokens,
    cast(min(day) as varchar) as first_day,
    cast(max(day) as varchar) as last_day
  from token_daily_top_flows
`;

type FlowSummary = {
  leaderboard_rows: number;
  tokens: number;
  first_day: string;
  last_day: string;
};

const createTokenDailyTotalsTableSql = `
  create table if not exists token_daily_totals (
    chain varchar,
    day date,
    token_address varchar,
    transfer_count bigint,
    amount_native_sum double,
    avg_amount_native double
  )
`;

const createTokenDailyTopFlowsTableSql = `
  create table if not exists token_daily_top_flows (
    chain varchar,
    day date,
    token_address varchar,
    token_name varchar,
    token_symbol varchar,
    token_decimals integer,
    target_source varchar,
    coingecko_id varchar,
    coingecko_name varchar,
    coingecko_symbol varchar,
    direction varchar,
    address varchar,
    flow_rank bigint,
    transfer_count bigint,
    amount_native_sum double,
    avg_amount_native double
  )
`;

const deleteDayRangeSql = ({
  table,
  fromDay,
  toDay,
}: {
  table: string;
  fromDay?: string;
  toDay?: string;
}) =>
  fromDay || toDay
    ? `
      delete from ${table}
      where true
        ${fromDay ? `and day >= date '${escapeSqlString(fromDay)}'` : ""}
        ${toDay ? `and day <= date '${escapeSqlString(toDay)}'` : ""}
      `
    : `delete from ${table}`;

const resolveBlockRange = async ({
  chain,
  fromDay,
  toDay,
}: {
  chain: Chain;
  fromDay?: string;
  toDay?: string;
}) => {
  if (!fromDay && !toDay) {
    return {};
  }

  const rpcUrl = resolveRpcUrl(chain);
  const sqdUrl = resolveSqdUrl(chain);
  const latestHeight = Number.parseInt(
    await fetch(`${sqdUrl}/height`).then(async (response) => {
      if (!response.ok) {
        throw new Error(`SQD error for ${chain}: ${response.status} ${response.statusText}`);
      }

      return response.text();
    }),
    10,
  );

  const fromBlock =
    fromDay === undefined
      ? undefined
      : await findBlockByTimestamp({
          targetTimestamp: dayTimestamp(fromDay),
          latestBlock: latestHeight,
          rpcUrl,
        });

  const toBlock =
    toDay === undefined
      ? undefined
      : (
          await findBlockByTimestamp({
            targetTimestamp: dayTimestamp(nextDay(toDay)),
            latestBlock: latestHeight,
            rpcUrl,
          })
        ) - 1;

  return {
    fromBlock,
    toBlock: toBlock === undefined ? undefined : Math.max(0, toBlock),
  };
};

const buildChainFlows = async ({
  connection,
  chain,
}: {
  connection: DuckDBConnection;
  chain: Chain;
}) => {
  const cacheDest = getCacheDest(chain);
  const { fromDay, toDay } = dayRange();
  const { fromBlock, toBlock } = await resolveBlockRange({
    chain,
    fromDay,
    toDay,
  });
  const chunkUris = (
    await listTransferChunks({
      cacheDest,
      fromBlock,
      toBlock,
    })
  ).map((chunk) => chunk.cacheUri);

  if (chunkUris.length === 0) {
    logLine("skipped flow build with no transfer chunks", { chain });
    return;
  }

  logLine("starting flows build", {
    chain,
    chunks: chunkUris.length,
    from_day: fromDay,
    to_day: toDay,
    from_block: fromBlock,
    to_block: toBlock,
  });

  logLine("building flows stage", { chain, stage: "manifest", chunks: chunkUris.length });
  await run(
    connection,
    manifestTableSql({
      manifestUri: getTokenManifestPath({ cacheDest }),
    }),
  );

  logLine("building flows stage", { chain, stage: "source_table" });
  await run(connection, sourceViewSql({ chain, chunkUris, fromDay, toDay }));

  logLine("building flows stage", { chain, stage: "daily_totals" });
  await run(connection, createTokenDailyTotalsTableSql);
  await run(connection, deleteDayRangeSql({ table: "token_daily_totals", fromDay, toDay }));
  await run(connection, `insert into token_daily_totals ${dailyTokenTotalsSelectSql}`);

  logLine("building flows stage", { chain, stage: "daily_top_flows" });
  await run(connection, createTokenDailyTopFlowsTableSql);
  await run(connection, deleteDayRangeSql({ table: "token_daily_top_flows", fromDay, toDay }));
  await run(connection, `insert into token_daily_top_flows ${dailyTopFlowsSelectSql}`);

  const [summary] = await rows<FlowSummary>(connection, summarySql);

  logLine("built flows", {
    chain,
    chunks: chunkUris.length,
    leaderboard_rows: summary?.leaderboard_rows,
    tokens: summary?.tokens,
    first_day: summary?.first_day,
    last_day: summary?.last_day,
  });
};

const main = async () => {
  const chain = selectedChain(process.argv.slice(2));
  const databasePath = outputPath(chain);
  await mkdir(dirname(databasePath), { recursive: true });
  const connection = await createConnection(databasePath);

  try {
    await configureDuckDb({ connection, chain });

    if (isS3Uri(getCacheDest(chain))) {
      await configureS3(connection, process.env);
    }

    await buildChainFlows({ connection, chain });

    logLine("flows ready", {
      chain,
      database: databasePath,
    });
  } finally {
    connection.closeSync();
  }
};

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logLine("build flows failed", { error: message });
  process.exitCode = 1;
});
