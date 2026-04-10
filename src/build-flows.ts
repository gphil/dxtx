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
const envFlag = (key: string) => {
  const value = process.env[key];
  return value === "1" || value === "true";
};
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

const daySql = "cast(date_trunc('day', timezone('UTC', to_timestamp(block_timestamp / 1000.0))) as date)";

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
  fromBlock,
  toBlock,
}: {
  chain: Chain;
  chunkUris: string[];
  fromBlock?: number;
  toBlock?: number;
}) => `
  create or replace temp table source_transfers as
  select
    '${escapeSqlString(chain)}' as chain,
    ${daySql} as day,
    token_address,
    transaction_hash,
    from_address,
    to_address,
    coalesce(amount_double, try_cast(amount_text as double)) as amount_native,
    block_number
  from read_parquet(${sqlStringList(chunkUris)})
  where true
    ${fromBlock !== undefined ? `and block_number >= ${fromBlock}` : ""}
    ${toBlock !== undefined ? `and block_number <= ${toBlock}` : ""}
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

const dailyAddressFlowsSelectSql = `
  with senders as (
    select
      chain,
      day,
      token_address,
      from_address as address,
      count(*) as sent_transfer_count,
      sum(amount_native) as sent_amount_native_sum
    from source_transfers
    where amount_native is not null
      and from_address is not null
    group by 1, 2, 3, 4
  ),
  recipients as (
    select
      chain,
      day,
      token_address,
      to_address as address,
      count(*) as received_transfer_count,
      sum(amount_native) as received_amount_native_sum
    from source_transfers
    where amount_native is not null
      and to_address is not null
    group by 1, 2, 3, 4
  ),
  addresses as (
    select
      coalesce(senders.chain, recipients.chain) as chain,
      coalesce(senders.day, recipients.day) as day,
      coalesce(senders.token_address, recipients.token_address) as token_address,
      coalesce(senders.address, recipients.address) as address,
      coalesce(senders.sent_transfer_count, 0) as sent_transfer_count,
      coalesce(recipients.received_transfer_count, 0) as received_transfer_count,
      coalesce(senders.sent_amount_native_sum, 0) as sent_amount_native_sum,
      coalesce(recipients.received_amount_native_sum, 0) as received_amount_native_sum
    from senders
    full outer join recipients
      on recipients.chain = senders.chain
     and recipients.day = senders.day
     and recipients.token_address = senders.token_address
     and recipients.address = senders.address
  )
  select
    addresses.chain,
    addresses.day,
    addresses.token_address,
    manifest.token_name,
    manifest.token_symbol,
    manifest.token_decimals,
    manifest.target_source,
    manifest.coingecko_id,
    manifest.coingecko_name,
    manifest.coingecko_symbol,
    addresses.address,
    addresses.sent_transfer_count,
    addresses.received_transfer_count,
    addresses.sent_transfer_count + addresses.received_transfer_count as total_transfer_count,
    addresses.sent_amount_native_sum,
    addresses.received_amount_native_sum,
    addresses.sent_amount_native_sum + addresses.received_amount_native_sum as gross_amount_native_sum,
    addresses.received_amount_native_sum - addresses.sent_amount_native_sum as net_amount_native_sum
  from addresses
  left join token_manifest as manifest
    on manifest.chain = addresses.chain
   and manifest.address = addresses.token_address
`;

const dailyTopFlowsSelectSql = `
  with flows as (
    select
      chain,
      day,
      token_address,
      token_name,
      token_symbol,
      token_decimals,
      target_source,
      coingecko_id,
      coingecko_name,
      coingecko_symbol,
      'sender' as direction,
      address,
      sent_transfer_count as transfer_count,
      sent_amount_native_sum as amount_native_sum,
      case
        when sent_transfer_count = 0 then null
        else sent_amount_native_sum / sent_transfer_count
      end as avg_amount_native
    from token_daily_address_flows
    where sent_transfer_count > 0
    union all
    select
      chain,
      day,
      token_address,
      token_name,
      token_symbol,
      token_decimals,
      target_source,
      coingecko_id,
      coingecko_name,
      coingecko_symbol,
      'recipient' as direction,
      address,
      received_transfer_count as transfer_count,
      received_amount_native_sum as amount_native_sum,
      case
        when received_transfer_count = 0 then null
        else received_amount_native_sum / received_transfer_count
      end as avg_amount_native
    from token_daily_address_flows
    where received_transfer_count > 0
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
    chain,
    day,
    token_address,
    token_name,
    token_symbol,
    token_decimals,
    target_source,
    coingecko_id,
    coingecko_name,
    coingecko_symbol,
    direction,
    address,
    flow_rank,
    transfer_count,
    amount_native_sum,
    avg_amount_native
  from ranked
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

type SourceSummary = {
  rows: number;
  first_day: string;
  last_day: string;
};

const sourceSummarySql = `
  select
    count(*) as rows,
    cast(min(day) as varchar) as first_day,
    cast(max(day) as varchar) as last_day
  from source_transfers
`;

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

const createTokenDailyAddressFlowsTableSql = `
  create table if not exists token_daily_address_flows (
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
    address varchar,
    sent_transfer_count bigint,
    received_transfer_count bigint,
    total_transfer_count bigint,
    sent_amount_native_sum double,
    received_amount_native_sum double,
    gross_amount_native_sum double,
    net_amount_native_sum double
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

const dropTableSql = (table: string) => `drop table if exists ${table}`;

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
  const reset = envFlag("ANALYTICS_RESET");
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
    reset: reset ? 1 : undefined,
    from_day: fromDay,
    to_day: toDay,
    from_block: fromBlock,
    to_block: toBlock,
  });

  if (reset) {
    logLine("building flows stage", { chain, stage: "reset" });
    await run(connection, dropTableSql("token_daily_top_flows"));
    await run(connection, dropTableSql("token_daily_address_flows"));
    await run(connection, dropTableSql("token_daily_totals"));
    await run(connection, dropTableSql("token_manifest"));
  }

  logLine("building flows stage", { chain, stage: "manifest", chunks: chunkUris.length });
  await run(
    connection,
    manifestTableSql({
      manifestUri: getTokenManifestPath({ cacheDest }),
    }),
  );

  logLine("building flows stage", { chain, stage: "source_table" });
  await run(connection, sourceViewSql({ chain, chunkUris, fromBlock, toBlock }));
  const [sourceSummary] = await rows<SourceSummary>(connection, sourceSummarySql);
  logLine("built flows source", {
    chain,
    source_rows: sourceSummary?.rows,
    first_day: sourceSummary?.first_day,
    last_day: sourceSummary?.last_day,
  });

  logLine("building flows stage", { chain, stage: "daily_totals" });
  await run(connection, createTokenDailyTotalsTableSql);
  await run(connection, deleteDayRangeSql({ table: "token_daily_totals", fromDay, toDay }));
  await run(connection, `insert into token_daily_totals ${dailyTokenTotalsSelectSql}`);

  logLine("building flows stage", { chain, stage: "daily_address_flows" });
  await run(connection, createTokenDailyAddressFlowsTableSql);
  await run(connection, deleteDayRangeSql({ table: "token_daily_address_flows", fromDay, toDay }));
  await run(connection, `insert into token_daily_address_flows ${dailyAddressFlowsSelectSql}`);

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
