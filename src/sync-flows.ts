import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { DuckDBInstance, type DuckDBConnection } from "@duckdb/node-api";
import { Client } from "pg";
import { getCacheDest, getTokenManifestPath, isS3Uri, listTransferChunks } from "./cache.js";
import { resolveRpcUrl, resolveSqdUrl, supportedChains } from "./chains.js";
import { findBlockByTimestamp } from "./evm.js";
import { escapeSqlString } from "./format.js";
import { logLine } from "./log.js";
import { ensureServingSchema } from "./serving-schema.js";
import type { Chain } from "./types.js";

const flowWindows = [1, 7, 30, 90] as const;
const daySql = "cast(date_trunc('day', timezone('UTC', to_timestamp(block_timestamp / 1000.0))) as date)";
type Network = "eth" | "base" | "arbitrum" | "optimism" | "polygon" | "avalanche" | "bsc" | "unichain";
const networkByChain: Record<Chain, Network> = {
  ethereum: "eth",
  base: "base",
  arbitrum: "arbitrum",
  optimism: "optimism",
  polygon: "polygon",
  avalanche: "avalanche",
  bsc: "bsc",
  unichain: "unichain",
};
const networkName = (chain: Chain): Network => networkByChain[chain];
const normalizeEnvName = (chain: Chain) => chain.toUpperCase();
const chainEnvValue = (baseKey: string, chain: Chain) =>
  process.env[`${baseKey}_${normalizeEnvName(chain)}`] ?? process.env[baseKey];

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
const hasValue = (value: string | undefined) => value !== undefined && value !== "";
const postgresConnectionString = () => {
  const directUrl = envValue("FLOWS_DATABASE_URL");

  if (hasValue(directUrl)) {
    return directUrl;
  }

  const dbUser = envValue("DB_USER");
  const dbPass = envValue("DB_PASS");
  const dbName = envValue("DB_NAME");
  const dbPort = envValue("DB_PORT");
  const dbHost = envValue("DB_HOST");

  if (![dbUser, dbPass, dbName, dbPort, dbHost].some(hasValue)) {
    return undefined;
  }

  const user = encodeURIComponent(dbUser || "squid");
  const password = encodeURIComponent(dbPass || "squid");
  const host = dbHost || "127.0.0.1";
  const port = dbPort || "15432";
  const database = encodeURIComponent(dbName || "dxtx_flows");

  return `postgresql://${user}:${password}@${host}:${port}/${database}`;
};

const splitArgs = (values: string[]) =>
  values.flatMap((value) => value.split(",")).map((value) => value.trim()).filter(Boolean);

const isChain = (value: string): value is Chain => supportedChains.includes(value as Chain);

const selectedChain = (args: string[]): Chain => {
  const requested = splitArgs(args);

  if (requested.length === 0) {
    return "ethereum";
  }

  const chains = [...new Set(requested.filter(isChain))];

  if (chains.length === 0) {
    throw new Error(`no supported chains selected; expected one of ${supportedChains.join(",")}`);
  }

  if (chains.length > 1) {
    throw new Error(`sync:flows currently supports one chain at a time; received ${chains.join(",")}`);
  }

  const [chain] = chains;

  if (!chain) {
    throw new Error("no chain selected");
  }

  return chain;
};

const nextDay = (value: string) =>
  new Date(Date.parse(`${value}T00:00:00Z`) + 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

const dayTimestamp = (value: string) => Math.floor(Date.parse(`${value}T00:00:00Z`) / 1_000);
const isBeforeDay = (left: string, right: string) => left.localeCompare(right) < 0;

const sqlStringList = (values: string[]) =>
  `[${values.map((value) => `'${escapeSqlString(value)}'`).join(", ")}]`;

const localDatabasePath = (chain: Chain) =>
  process.env.FLOW_SYNC_DB_PATH || `./analytics/${chain}-flow-sync.duckdb`;

const tempDirectory = (chain: Chain) =>
  process.env.ANALYTICS_TEMP_DIRECTORY || `./analytics/${chain}-flow-sync-tmp`;

const createConnection = async (databasePath: string) => {
  const instance = await DuckDBInstance.create(databasePath);
  return instance.connect();
};

const run = async (connection: DuckDBConnection, sql: string) => {
  await connection.run(sql);
};

const rows = async <T>(connection: DuckDBConnection, sql: string) =>
  (await connection.runAndReadAll(sql)).getRowObjectsJS() as T[];

const configureDuckDb = async ({
  connection,
  chain,
}: {
  connection: DuckDBConnection;
  chain: Chain;
}) => {
  const memoryLimit = chainEnvValue("ANALYTICS_MEMORY_LIMIT", chain) || "4GB";
  const threads = chainEnvValue("ANALYTICS_THREADS", chain);
  const tempDir = tempDirectory(chain);

  await mkdir(tempDir, { recursive: true });
  await run(connection, `SET memory_limit='${escapeSqlString(memoryLimit)}'`);
  await run(connection, `SET temp_directory='${escapeSqlString(tempDir)}'`);
  await run(connection, "SET preserve_insertion_order=false");

  if (threads) {
    await run(connection, `SET threads=${Number.parseInt(threads, 10)}`);
  }
};

const configureS3 = async (connection: DuckDBConnection, env: NodeJS.ProcessEnv) => {
  const endpointValue = assertEnv(env.CACHE_S3_ENDPOINT, "CACHE_S3_ENDPOINT");
  const endpoint = endpointValue.replace(/^[a-z]+:\/\//i, "").replace(/\/+$/, "");
  const region = assertEnv(env.CACHE_S3_REGION, "CACHE_S3_REGION");
  const accessKeyId = assertEnv(env.CACHE_S3_ACCESS_KEY_ID ?? env.AWS_ACCESS_KEY_ID, "CACHE_S3_ACCESS_KEY_ID");
  const secretAccessKey = assertEnv(
    env.CACHE_S3_SECRET_ACCESS_KEY ?? env.AWS_SECRET_ACCESS_KEY,
    "CACHE_S3_SECRET_ACCESS_KEY",
  );
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

const resolveDayBlockRange = async ({
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

const createProcessedChunksTableSql = `
  create table if not exists processed_flow_chunks (
    chain varchar,
    cache_uri varchar,
    from_block bigint,
    to_block bigint,
    processed_at timestamp
  )
`;

const createTokenManifestTableSql = `
  create table if not exists token_manifest (
    chain varchar,
    address varchar,
    target_source varchar,
    coingecko_id varchar,
    coingecko_name varchar,
    coingecko_symbol varchar,
    token_name varchar,
    token_symbol varchar,
    token_decimals integer
  )
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

const createTokenFlowLeaderboardsCurrentTableSql = `
  create table if not exists token_flow_leaderboards_current (
    chain varchar,
    token_address varchar,
    token_name varchar,
    token_symbol varchar,
    token_decimals integer,
    target_source varchar,
    coingecko_id varchar,
    coingecko_name varchar,
    coingecko_symbol varchar,
    window_days integer,
    window_start_day date,
    window_end_day date,
    is_partial_day integer,
    metric varchar,
    address varchar,
    flow_rank bigint,
    transfer_count bigint,
    amount_native_sum double,
    avg_amount_native double
  )
`;

const ensureLocalSchema = async (connection: DuckDBConnection) => {
  await run(connection, createProcessedChunksTableSql);
  await run(connection, createTokenManifestTableSql);
  await run(connection, createTokenDailyTotalsTableSql);
  await run(connection, createTokenDailyAddressFlowsTableSql);
  await run(connection, createTokenDailyTopFlowsTableSql);
  await run(connection, createTokenFlowLeaderboardsCurrentTableSql);
};

const sourceTransfersSql = ({
  chain,
  chunkUris,
}: {
  chain: Chain;
  chunkUris: string[];
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
`;

const manifestTableSql = ({ manifestUri }: { manifestUri: string }) => `
  create or replace temp table source_token_manifest as
  select *
  from read_parquet('${escapeSqlString(manifestUri)}')
`;

const replaceTokenManifestSql = `
  delete from token_manifest;
  insert into token_manifest (
    chain,
    address,
    target_source,
    coingecko_id,
    coingecko_name,
    coingecko_symbol,
    token_name,
    token_symbol,
    token_decimals
  )
  select
    chain,
    address,
    target_source,
    coingecko_id,
    coingecko_name,
    coingecko_symbol,
    token_name,
    token_symbol,
    token_decimals
  from source_token_manifest;
`;

const sourceDaysTokensSql = `
  create or replace temp table source_days_tokens as
  select distinct
    chain,
    day,
    token_address
  from source_transfers
`;

const sourceTokensSql = `
  create or replace temp table source_tokens as
  select distinct
    chain,
    token_address
  from source_transfers
`;

const deltaDailyTotalsSql = `
  create or replace temp table delta_token_daily_totals as
  select
    chain,
    day,
    token_address,
    count(*) as transfer_count,
    sum(amount_native) as amount_native_sum
  from source_transfers
  where amount_native is not null
  group by 1, 2, 3
`;

const applyDailyTotalsDeltaSql = `
  update token_daily_totals as target
  set
    transfer_count = target.transfer_count + delta.transfer_count,
    amount_native_sum = target.amount_native_sum + delta.amount_native_sum,
    avg_amount_native =
      (target.amount_native_sum + delta.amount_native_sum)
      / nullif(target.transfer_count + delta.transfer_count, 0)
  from delta_token_daily_totals as delta
  where target.chain = delta.chain
    and target.day = delta.day
    and target.token_address = delta.token_address;

  insert into token_daily_totals
  select
    delta.chain,
    delta.day,
    delta.token_address,
    delta.transfer_count,
    delta.amount_native_sum,
    delta.amount_native_sum / nullif(delta.transfer_count, 0) as avg_amount_native
  from delta_token_daily_totals as delta
  left join token_daily_totals as target
    on target.chain = delta.chain
   and target.day = delta.day
   and target.token_address = delta.token_address
  where target.chain is null;
`;

const deltaDailyAddressFlowsSql = `
  create or replace temp table delta_token_daily_address_flows as
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

const applyDailyAddressFlowsDeltaSql = `
  update token_daily_address_flows as target
  set
    sent_transfer_count = target.sent_transfer_count + delta.sent_transfer_count,
    received_transfer_count = target.received_transfer_count + delta.received_transfer_count,
    total_transfer_count =
      target.sent_transfer_count + target.received_transfer_count
      + delta.sent_transfer_count + delta.received_transfer_count,
    sent_amount_native_sum = target.sent_amount_native_sum + delta.sent_amount_native_sum,
    received_amount_native_sum = target.received_amount_native_sum + delta.received_amount_native_sum,
    gross_amount_native_sum =
      target.sent_amount_native_sum + target.received_amount_native_sum
      + delta.sent_amount_native_sum + delta.received_amount_native_sum,
    net_amount_native_sum =
      target.received_amount_native_sum - target.sent_amount_native_sum
      + delta.received_amount_native_sum - delta.sent_amount_native_sum,
    token_name = coalesce(delta.token_name, target.token_name),
    token_symbol = coalesce(delta.token_symbol, target.token_symbol),
    token_decimals = coalesce(delta.token_decimals, target.token_decimals),
    target_source = coalesce(delta.target_source, target.target_source),
    coingecko_id = coalesce(delta.coingecko_id, target.coingecko_id),
    coingecko_name = coalesce(delta.coingecko_name, target.coingecko_name),
    coingecko_symbol = coalesce(delta.coingecko_symbol, target.coingecko_symbol)
  from delta_token_daily_address_flows as delta
  where target.chain = delta.chain
    and target.day = delta.day
    and target.token_address = delta.token_address
    and target.address = delta.address;

  insert into token_daily_address_flows
  select
    delta.chain,
    delta.day,
    delta.token_address,
    delta.token_name,
    delta.token_symbol,
    delta.token_decimals,
    delta.target_source,
    delta.coingecko_id,
    delta.coingecko_name,
    delta.coingecko_symbol,
    delta.address,
    delta.sent_transfer_count,
    delta.received_transfer_count,
    delta.total_transfer_count,
    delta.sent_amount_native_sum,
    delta.received_amount_native_sum,
    delta.gross_amount_native_sum,
    delta.net_amount_native_sum
  from delta_token_daily_address_flows as delta
  left join token_daily_address_flows as target
    on target.chain = delta.chain
   and target.day = delta.day
   and target.token_address = delta.token_address
   and target.address = delta.address
  where target.chain is null;
`;

const refreshDailyTopFlowsSql = `
  delete from token_daily_top_flows as target
  where exists (
    select 1
    from source_days_tokens as affected
    where affected.chain = target.chain
      and affected.day = target.day
      and affected.token_address = target.token_address
  );

  insert into token_daily_top_flows
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
      and exists (
        select 1
        from source_days_tokens as affected
        where affected.chain = token_daily_address_flows.chain
          and affected.day = token_daily_address_flows.day
          and affected.token_address = token_daily_address_flows.token_address
      )
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
      and exists (
        select 1
        from source_days_tokens as affected
        where affected.chain = token_daily_address_flows.chain
          and affected.day = token_daily_address_flows.day
          and affected.token_address = token_daily_address_flows.token_address
      )
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
  where flow_rank <= 50
`;

const windowsSql = flowWindows.map((value, index) => `${index === 0 ? "" : "union all "}select ${value} as window_days`).join("\n");

const refreshLeaderboardsSql = ({
  latestDay,
  currentDay,
  fullRebuild,
}: {
  latestDay: string;
  currentDay: string;
  fullRebuild: boolean;
}) => `
  ${fullRebuild
    ? "delete from token_flow_leaderboards_current;"
    : `
      delete from token_flow_leaderboards_current as target
      where exists (
        select 1
        from source_tokens as affected
        where affected.chain = target.chain
          and affected.token_address = target.token_address
      );
    `}

  insert into token_flow_leaderboards_current
  with windows as (
    ${windowsSql}
  ),
  windowed as (
    select
      flows.chain,
      flows.token_address,
      flows.token_name,
      flows.token_symbol,
      flows.token_decimals,
      flows.target_source,
      flows.coingecko_id,
      flows.coingecko_name,
      flows.coingecko_symbol,
      windows.window_days,
      cast(date '${escapeSqlString(latestDay)}' - ((windows.window_days - 1) * interval 1 day) as date) as window_start_day,
      date '${escapeSqlString(latestDay)}' as window_end_day,
      flows.address,
      sum(flows.sent_transfer_count) as sent_transfer_count,
      sum(flows.received_transfer_count) as received_transfer_count,
      sum(flows.total_transfer_count) as total_transfer_count,
      sum(flows.sent_amount_native_sum) as sent_amount_native_sum,
      sum(flows.received_amount_native_sum) as received_amount_native_sum,
      sum(flows.gross_amount_native_sum) as gross_amount_native_sum,
      sum(flows.net_amount_native_sum) as net_amount_native_sum
    from token_daily_address_flows as flows
    cross join windows
    where flows.day between
      cast(date '${escapeSqlString(latestDay)}' - ((windows.window_days - 1) * interval 1 day) as date)
      and date '${escapeSqlString(latestDay)}'
      ${fullRebuild ? "" : "and exists (select 1 from source_tokens as affected where affected.chain = flows.chain and affected.token_address = flows.token_address)"}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  metrics as (
    select
      chain,
      token_address,
      token_name,
      token_symbol,
      token_decimals,
      target_source,
      coingecko_id,
      coingecko_name,
      coingecko_symbol,
      window_days,
      window_start_day,
      window_end_day,
      ${latestDay === currentDay ? 1 : 0} as is_partial_day,
      'gross_sender' as metric,
      address,
      sent_transfer_count as transfer_count,
      sent_amount_native_sum as amount_native_sum,
      case
        when sent_transfer_count = 0 then null
        else sent_amount_native_sum / sent_transfer_count
      end as avg_amount_native
    from windowed
    where sent_transfer_count > 0
    union all
    select
      chain,
      token_address,
      token_name,
      token_symbol,
      token_decimals,
      target_source,
      coingecko_id,
      coingecko_name,
      coingecko_symbol,
      window_days,
      window_start_day,
      window_end_day,
      ${latestDay === currentDay ? 1 : 0} as is_partial_day,
      'gross_recipient' as metric,
      address,
      received_transfer_count as transfer_count,
      received_amount_native_sum as amount_native_sum,
      case
        when received_transfer_count = 0 then null
        else received_amount_native_sum / received_transfer_count
      end as avg_amount_native
    from windowed
    where received_transfer_count > 0
    union all
    select
      chain,
      token_address,
      token_name,
      token_symbol,
      token_decimals,
      target_source,
      coingecko_id,
      coingecko_name,
      coingecko_symbol,
      window_days,
      window_start_day,
      window_end_day,
      ${latestDay === currentDay ? 1 : 0} as is_partial_day,
      'net_inflow' as metric,
      address,
      total_transfer_count as transfer_count,
      net_amount_native_sum as amount_native_sum,
      case
        when total_transfer_count = 0 then null
        else net_amount_native_sum / total_transfer_count
      end as avg_amount_native
    from windowed
    where net_amount_native_sum > 0
    union all
    select
      chain,
      token_address,
      token_name,
      token_symbol,
      token_decimals,
      target_source,
      coingecko_id,
      coingecko_name,
      coingecko_symbol,
      window_days,
      window_start_day,
      window_end_day,
      ${latestDay === currentDay ? 1 : 0} as is_partial_day,
      'net_outflow' as metric,
      address,
      total_transfer_count as transfer_count,
      -net_amount_native_sum as amount_native_sum,
      case
        when total_transfer_count = 0 then null
        else (-net_amount_native_sum) / total_transfer_count
      end as avg_amount_native
    from windowed
    where net_amount_native_sum < 0
  ),
  ranked as (
    select
      metrics.*,
      row_number() over (
        partition by chain, token_address, window_days, metric
        order by amount_native_sum desc, transfer_count desc, address asc
      ) as flow_rank
    from metrics
  )
  select
    chain,
    token_address,
    token_name,
    token_symbol,
    token_decimals,
    target_source,
    coingecko_id,
    coingecko_name,
    coingecko_symbol,
    window_days,
    window_start_day,
    window_end_day,
    is_partial_day,
    metric,
    address,
    flow_rank,
    transfer_count,
    amount_native_sum,
    avg_amount_native
  from ranked
  where flow_rank <= 50
`;

const markProcessedChunksSql = (chain: Chain, chunks: Array<{ cacheUri: string; fromBlock: number; toBlock: number }>) => `
  insert into processed_flow_chunks
  values
  ${chunks
    .map(
      (chunk) =>
        `('${escapeSqlString(chain)}', '${escapeSqlString(chunk.cacheUri)}', ${chunk.fromBlock}, ${chunk.toBlock}, current_timestamp)`,
    )
    .join(",\n")}
`;

type ProcessedChunkRow = {
  cache_uri: string;
};

type LatestDayRow = {
  latest_day: string | null;
};

type LatestExportDayRow = {
  latest_day: string | null;
};

type TokenExportRow = {
  token_address: string;
};

type DailyTotalExportRow = {
  network: string;
  token_address: string;
  day: string;
  transfer_count: bigint | number;
  amount_native_sum: number;
  avg_amount_native: number;
  is_partial_day: boolean;
  as_of_ts: string;
};

type DailyAddressFlowExportRow = {
  network: string;
  token_address: string;
  day: string;
  token_name: string | null;
  token_symbol: string | null;
  token_decimals: number | null;
  target_source: string | null;
  coingecko_id: string | null;
  coingecko_name: string | null;
  coingecko_symbol: string | null;
  address: string;
  sent_transfer_count: bigint | number;
  received_transfer_count: bigint | number;
  total_transfer_count: bigint | number;
  sent_amount_native_sum: number;
  received_amount_native_sum: number;
  gross_amount_native_sum: number;
  net_amount_native_sum: number;
  is_partial_day: boolean;
  as_of_ts: string;
};

type LeaderboardExportRow = {
  network: string;
  token_address: string;
  token_name: string | null;
  token_symbol: string | null;
  token_decimals: number | null;
  target_source: string | null;
  coingecko_id: string | null;
  coingecko_name: string | null;
  coingecko_symbol: string | null;
  window_days: number;
  window_start_day: string;
  window_end_day: string;
  is_partial_day: boolean;
  metric: string;
  address: string;
  flow_rank: bigint | number;
  transfer_count: bigint | number;
  amount_native_sum: number;
  avg_amount_native: number | null;
  as_of_ts: string;
};

const selectProcessedChunkUris = async (connection: DuckDBConnection) =>
  new Set((await rows<ProcessedChunkRow>(connection, "select cache_uri from processed_flow_chunks")).map((row) => row.cache_uri));

const selectLatestLeaderboardDay = async (connection: DuckDBConnection) => {
  const [row] = await rows<LatestDayRow>(
    connection,
    "select cast(max(window_end_day) as varchar) as latest_day from token_flow_leaderboards_current",
  );
  return row?.latest_day ?? null;
};

const selectLatestTotalsDay = async (connection: DuckDBConnection) => {
  const [row] = await rows<LatestDayRow>(
    connection,
    "select cast(max(day) as varchar) as latest_day from token_daily_totals",
  );
  return row?.latest_day ?? null;
};

const dailyTotalsExportSql = ({
  asOfTs,
  latestDay,
  fullRebuild,
  network,
}: {
  asOfTs: string;
  latestDay: string;
  fullRebuild: boolean;
  network: Network;
}) => `
  select
    '${escapeSqlString(network)}' as network,
    totals.token_address,
    cast(totals.day as varchar) as day,
    totals.transfer_count,
    totals.amount_native_sum,
    totals.avg_amount_native,
    totals.day = date '${escapeSqlString(latestDay)}' as is_partial_day,
    '${escapeSqlString(asOfTs)}' as as_of_ts
  from token_daily_totals as totals
  where ${fullRebuild ? "true" : "exists (select 1 from source_days_tokens as affected where affected.chain = totals.chain and affected.day = totals.day and affected.token_address = totals.token_address)"}
`;

const dailyAddressFlowsExportSql = ({
  asOfTs,
  latestDay,
  fullRebuild,
  network,
}: {
  asOfTs: string;
  latestDay: string;
  fullRebuild: boolean;
  network: Network;
}) => `
  select
    '${escapeSqlString(network)}' as network,
    flows.token_address,
    cast(flows.day as varchar) as day,
    flows.token_name,
    flows.token_symbol,
    flows.token_decimals,
    flows.target_source,
    flows.coingecko_id,
    flows.coingecko_name,
    flows.coingecko_symbol,
    flows.address,
    flows.sent_transfer_count,
    flows.received_transfer_count,
    flows.total_transfer_count,
    flows.sent_amount_native_sum,
    flows.received_amount_native_sum,
    flows.gross_amount_native_sum,
    flows.net_amount_native_sum,
    flows.day = date '${escapeSqlString(latestDay)}' as is_partial_day,
    '${escapeSqlString(asOfTs)}' as as_of_ts
  from token_daily_address_flows as flows
  where ${fullRebuild ? "true" : "exists (select 1 from source_days_tokens as affected where affected.chain = flows.chain and affected.day = flows.day and affected.token_address = flows.token_address)"}
`;

const leaderboardExportSql = ({
  asOfTs,
  fullRebuild,
  network,
}: {
  asOfTs: string;
  fullRebuild: boolean;
  network: Network;
}) => `
  select
    '${escapeSqlString(network)}' as network,
    token_address,
    token_name,
    token_symbol,
    token_decimals,
    target_source,
    coingecko_id,
    coingecko_name,
    coingecko_symbol,
    window_days,
    cast(window_start_day as varchar) as window_start_day,
    cast(window_end_day as varchar) as window_end_day,
    is_partial_day = 1 as is_partial_day,
    metric,
    address,
    flow_rank,
    transfer_count,
    amount_native_sum,
    avg_amount_native,
    '${escapeSqlString(asOfTs)}' as as_of_ts
  from token_flow_leaderboards_current
  where ${fullRebuild ? "true" : "exists (select 1 from source_tokens as affected where affected.chain = token_flow_leaderboards_current.chain and affected.token_address = token_flow_leaderboards_current.token_address)"}
`;

const affectedTokensSql = `
  select distinct
    token_address
  from source_tokens
  order by token_address
`;

const normalizePgValue = (value: unknown): unknown =>
  typeof value === "bigint" ? value.toString() : value;

const chunkRows = <T>(values: T[], size: number) =>
  Array.from({ length: Math.ceil(values.length / size) }, (_, index) =>
    values.slice(index * size, (index + 1) * size),
  );

const insertRowsSql = ({
  table,
  columns,
  rows,
  conflict,
  updates,
}: {
  table: string;
  columns: string[];
  rows: Record<string, unknown>[];
  conflict: string[];
  updates: string[];
}) => {
  const values = rows
    .map(
      (_, rowIndex) =>
        `(${columns.map((__, columnIndex) => `$${rowIndex * columns.length + columnIndex + 1}`).join(", ")})`,
    )
    .join(", ");
  const params = rows.flatMap((row) => columns.map((column) => normalizePgValue(row[column])));

  return {
    text: `
      insert into ${table} (${columns.join(", ")})
      values ${values}
      on conflict (${conflict.join(", ")}) do update
      set ${updates.map((column) => `${column} = excluded.${column}`).join(", ")}
    `,
    params,
  };
};

const deleteAffectedLeaderboardRows = async ({
  client,
  network,
  tokens,
  fullRebuild,
}: {
  client: Client;
  network: Network;
  tokens: TokenExportRow[];
  fullRebuild: boolean;
}) => {
  if (fullRebuild) {
    await client.query("delete from token_flow_leaderboards where network = $1", [network]);
    return;
  }

  if (tokens.length === 0) {
    return;
  }

  const values = tokens.map((_, index) => `($1, $${index + 2})`).join(", ");
  const params = [network, ...tokens.map((token) => token.token_address)];

  await client.query(
    `
      delete from token_flow_leaderboards as target
      using (values ${values}) as affected(network, token_address)
      where affected.network = target.network
        and affected.token_address = target.token_address
    `,
    params,
  );
};

const exportToPostgres = async ({
  connection,
  chain,
  fullRebuild,
  includeLeaderboards,
  latestDay,
}: {
  connection: DuckDBConnection;
  chain: Chain;
  fullRebuild: boolean;
  includeLeaderboards: boolean;
  latestDay: string;
}) => {
  const databaseUrl = postgresConnectionString();
  const network = networkName(chain);

  if (!databaseUrl) {
    return;
  }

  const asOfTs = new Date().toISOString();
  const dailyTotals = await rows<DailyTotalExportRow>(
    connection,
    dailyTotalsExportSql({ asOfTs, latestDay, fullRebuild, network }),
  );
  const dailyAddressFlows = await rows<DailyAddressFlowExportRow>(
    connection,
    dailyAddressFlowsExportSql({ asOfTs, latestDay, fullRebuild, network }),
  );
  const leaderboards = includeLeaderboards
    ? await rows<LeaderboardExportRow>(
        connection,
        leaderboardExportSql({ asOfTs, fullRebuild, network }),
      )
    : [];
  const affectedTokens =
    includeLeaderboards && !fullRebuild
      ? await rows<TokenExportRow>(connection, affectedTokensSql)
      : [];
  const client = new Client({ connectionString: databaseUrl });

  await client.connect();

  try {
    await client.query("begin");

    if (fullRebuild) {
      await client.query("delete from token_daily_address_flows where network = $1", [network]);
    }

    if (dailyTotals.length > 0) {
      const columns = [
        "network",
        "token_address",
        "day",
        "transfer_count",
        "amount_native_sum",
        "avg_amount_native",
        "is_partial_day",
        "as_of_ts",
      ];

      for (const chunk of chunkRows(dailyTotals, 1000)) {
        const { text, params } = insertRowsSql({
          table: "token_flow_daily_totals",
          columns,
          rows: chunk as unknown as Record<string, unknown>[],
          conflict: ["network", "token_address", "day"],
          updates: columns.slice(3),
        });
        await client.query(text, params);
      }
    }

    if (dailyAddressFlows.length > 0) {
      const columns = [
        "network",
        "token_address",
        "day",
        "token_name",
        "token_symbol",
        "token_decimals",
        "target_source",
        "coingecko_id",
        "coingecko_name",
        "coingecko_symbol",
        "address",
        "sent_transfer_count",
        "received_transfer_count",
        "total_transfer_count",
        "sent_amount_native_sum",
        "received_amount_native_sum",
        "gross_amount_native_sum",
        "net_amount_native_sum",
        "is_partial_day",
        "as_of_ts",
      ];

      for (const chunk of chunkRows(dailyAddressFlows, 1000)) {
        const { text, params } = insertRowsSql({
          table: "token_daily_address_flows",
          columns,
          rows: chunk as unknown as Record<string, unknown>[],
          conflict: ["network", "token_address", "day", "address"],
          updates: columns.slice(3),
        });
        await client.query(text, params);
      }
    }

    if (includeLeaderboards) {
      await deleteAffectedLeaderboardRows({
        client,
        network,
        tokens: affectedTokens,
        fullRebuild,
      });
    }

    if (includeLeaderboards && leaderboards.length > 0) {
      const columns = [
        "network",
        "token_address",
        "token_name",
        "token_symbol",
        "token_decimals",
        "target_source",
        "coingecko_id",
        "coingecko_name",
        "coingecko_symbol",
        "window_days",
        "window_start_day",
        "window_end_day",
        "is_partial_day",
        "metric",
        "address",
        "flow_rank",
        "transfer_count",
        "amount_native_sum",
        "avg_amount_native",
        "as_of_ts",
      ];

      for (const chunk of chunkRows(leaderboards, 1000)) {
        const { text, params } = insertRowsSql({
          table: "token_flow_leaderboards",
          columns,
          rows: chunk as unknown as Record<string, unknown>[],
          conflict: ["network", "token_address", "window_days", "metric", "flow_rank"],
          updates: columns.slice(2),
        });
        await client.query(text, params);
      }
    }

    await client.query("commit");
    logLine("published flows to postgres", {
      chain,
      daily_rows: dailyTotals.length,
      daily_address_flow_rows: dailyAddressFlows.length,
      leaderboard_rows: includeLeaderboards ? leaderboards.length : undefined,
      deferred_leaderboards: includeLeaderboards ? undefined : 1,
      full_rebuild: fullRebuild ? 1 : undefined,
    });
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    await client.end();
  }
};

const ensurePostgresSchema = async () => {
  if (!postgresConnectionString()) {
    return;
  }

  await ensureServingSchema();
};

const selectPostgresLatestTotalsDay = async (network: Network) => {
  const databaseUrl = postgresConnectionString();

  if (!databaseUrl) {
    return null;
  }

  const client = new Client({ connectionString: databaseUrl });

  await client.connect();

  try {
    const result = await client.query<LatestExportDayRow>(
      "select cast(max(day) as text) as latest_day from token_flow_daily_totals where network = $1",
      [network],
    );
    return result.rows[0]?.latest_day ?? null;
  } finally {
    await client.end();
  }
};

const resetLocalState = async (connection: DuckDBConnection) => {
  await run(connection, "drop table if exists processed_flow_chunks");
  await run(connection, "drop table if exists token_manifest");
  await run(connection, "drop table if exists token_daily_totals");
  await run(connection, "drop table if exists token_daily_address_flows");
  await run(connection, "drop table if exists token_daily_top_flows");
  await run(connection, "drop table if exists token_flow_leaderboards_current");
};

type SyncPassResult = {
  processedChunks: number;
  remainingChunks: number;
};

const syncPass = async ({
  connection,
  chain,
}: {
  connection: DuckDBConnection;
  chain: Chain;
}): Promise<SyncPassResult> => {
  const reset = envFlag("FLOW_SYNC_RESET");
  const maxChunks = Number.parseInt(chainEnvValue("FLOW_SYNC_MAX_CHUNKS", chain) || "32", 10);
  const fromDay = chainEnvValue("FLOW_SYNC_FROM_DAY", chain);
  const toDay = chainEnvValue("FLOW_SYNC_TO_DAY", chain);

  if (reset) {
    logLine("resetting local flow sync state", { chain });
    await resetLocalState(connection);
  }

  await ensureLocalSchema(connection);

  const { fromBlock, toBlock } = await resolveDayBlockRange({
    chain,
    fromDay,
    toDay,
  });
  const allChunks = await listTransferChunks({
    cacheDest: getCacheDest(chain),
    fromBlock,
    toBlock,
  });
  const processedChunkUris = await selectProcessedChunkUris(connection);
  const unprocessedChunks = allChunks.filter((chunk) => !processedChunkUris.has(chunk.cacheUri));
  const selectedChunks = unprocessedChunks.slice(0, Math.max(1, maxChunks));

  logLine("starting flow sync pass", {
    chain,
    known_chunks: allChunks.length,
    pending_chunks: unprocessedChunks.length,
    selected_chunks: selectedChunks.length,
    from_day: fromDay,
    to_day: toDay,
    from_block: fromBlock,
    to_block: toBlock,
  });

  if (selectedChunks.length === 0) {
    const latestDay = await selectLatestTotalsDay(connection);
    const network = networkName(chain);
    const exportedLatestDay = latestDay === null ? null : await selectPostgresLatestTotalsDay(network);

    logLine("flow sync idle", {
      chain,
      latest_day: latestDay ?? undefined,
      exported_latest_day: exportedLatestDay ?? undefined,
    });

    if (latestDay !== null && exportedLatestDay !== latestDay) {
      logLine("repairing postgres flow export", {
        chain,
        latest_day: latestDay,
        exported_latest_day: exportedLatestDay ?? undefined,
      });
      await exportToPostgres({
        connection,
        chain,
        fullRebuild: true,
        includeLeaderboards: true,
        latestDay,
      });
    }

    return {
      processedChunks: 0,
      remainingChunks: 0,
    };
  }

  await run(
    connection,
    manifestTableSql({
      manifestUri: getTokenManifestPath({ cacheDest: getCacheDest(chain) }),
    }),
  );
  await run(connection, replaceTokenManifestSql);
  await run(
    connection,
    sourceTransfersSql({
      chain,
      chunkUris: selectedChunks.map((chunk) => chunk.cacheUri),
    }),
  );
  await run(connection, sourceDaysTokensSql);
  await run(connection, sourceTokensSql);
  await run(connection, deltaDailyTotalsSql);
  await run(connection, applyDailyTotalsDeltaSql);
  await run(connection, deltaDailyAddressFlowsSql);
  await run(connection, applyDailyAddressFlowsDeltaSql);
  await run(connection, refreshDailyTopFlowsSql);
  await run(connection, markProcessedChunksSql(chain, selectedChunks));

  const latestDay = await selectLatestTotalsDay(connection);
  const remainingChunks = Math.max(0, unprocessedChunks.length - selectedChunks.length);

  if (!latestDay) {
    throw new Error(`no latest day found after flow sync for ${chain}`);
  }

  const currentDay = new Date().toISOString().slice(0, 10);
  const previousLeaderboardDay = await selectLatestLeaderboardDay(connection);
  const fullRebuild = previousLeaderboardDay !== latestDay;
  const includeLeaderboards = !(fullRebuild && remainingChunks > 0 && isBeforeDay(latestDay, currentDay));

  if (includeLeaderboards) {
    await run(
      connection,
      refreshLeaderboardsSql({
        latestDay,
        currentDay,
        fullRebuild,
      }),
    );
  } else {
    logLine("deferred flow leaderboard export", {
      chain,
      latest_day: latestDay,
      remaining_chunks: remainingChunks,
    });
  }

  await exportToPostgres({
    connection,
    chain,
    fullRebuild,
    includeLeaderboards,
    latestDay,
  });

  logLine("completed flow sync pass", {
    chain,
    processed_chunks: selectedChunks.length,
    remaining_chunks: remainingChunks,
    latest_day: latestDay,
    deferred_leaderboards: includeLeaderboards ? undefined : 1,
    full_rebuild: fullRebuild ? 1 : undefined,
  });

  return {
    processedChunks: selectedChunks.length,
    remainingChunks,
  };
};

const main = async () => {
  const chain = selectedChain(process.argv.slice(2));
  const databasePath = localDatabasePath(chain);
  await mkdir(dirname(databasePath), { recursive: true });
  const connection = await createConnection(databasePath);

  try {
    await configureDuckDb({ connection, chain });
    await ensurePostgresSchema();

    if (isS3Uri(getCacheDest(chain))) {
      await configureS3(connection, process.env);
    }

    const loop = envFlag("FLOW_SYNC_LOOP");
    const intervalSeconds = Number.parseInt(envValue("FLOW_SYNC_INTERVAL_SEC") || "300", 10);

    do {
      const result = await syncPass({ connection, chain });

      if (!loop) {
        break;
      }

      if (result.remainingChunks > 0) {
        continue;
      }

      await sleep(Math.max(1, intervalSeconds) * 1_000);
    } while (true);

    logLine("flow sync ready", {
      chain,
      database: databasePath,
    });
  } finally {
    connection.closeSync();
  }
};

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logLine("flow sync failed", { error: message });
  process.exitCode = 1;
});
