import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { DuckDBInstance, type DuckDBConnection } from "@duckdb/node-api";
import { getCacheDest, getTokenManifestPath, isS3Uri, listTransferChunks } from "./cache.js";
import { supportedChains } from "./chains.js";
import { escapeSqlString } from "./format.js";
import { logLine } from "./log.js";
import type { Chain } from "./types.js";

const assertEnv = (value: string | undefined, key: string) => {
  if (!value) {
    throw new Error(`${key} is required when CACHE_DEST points at s3://...`);
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
    throw new Error(`build:flow-prototype currently supports one chain at a time; received ${chains.join(",")}`);
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
  process.env.ANALYTICS_DB_PATH || `./analytics/${chain}-flow-prototype.duckdb`;

const sourceViewSql = ({
  chain,
  chunkUris,
}: {
  chain: Chain;
  chunkUris: string[];
}) => `
  create or replace temp view source_transfers as
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

const dailyTokenTotalsSql = `
  create or replace table token_daily_totals as
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

const dailyAddressFlowsSql = `
  create or replace table token_daily_address_flows as
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
  select *
  from senders
  union all
  select *
  from recipients
`;

const dailyTopFlowsSql = `
  create or replace table token_daily_top_flows as
  with ranked as (
    select
      flows.*,
      row_number() over (
        partition by chain, day, token_address, direction
        order by amount_native_sum desc, transfer_count desc, address asc
      ) as flow_rank
    from token_daily_address_flows as flows
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

type PrototypeSummary = {
  leaderboard_rows: number;
  tokens: number;
  first_day: string;
  last_day: string;
};

const buildChainPrototype = async ({
  connection,
  chain,
}: {
  connection: DuckDBConnection;
  chain: Chain;
}) => {
  const cacheDest = getCacheDest(chain);
  const chunkUris = (await listTransferChunks({ cacheDest })).map((chunk) => chunk.cacheUri);

  if (chunkUris.length === 0) {
    logLine("skipped flow prototype with no transfer chunks", { chain });
    return;
  }

  await run(
    connection,
    manifestTableSql({
      manifestUri: getTokenManifestPath({ cacheDest }),
    }),
  );
  await run(connection, sourceViewSql({ chain, chunkUris }));
  await run(connection, dailyTokenTotalsSql);
  await run(connection, dailyAddressFlowsSql);
  await run(connection, dailyTopFlowsSql);

  const [summary] = await rows<PrototypeSummary>(connection, summarySql);

  logLine("built flow prototype", {
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
    if (isS3Uri(getCacheDest(chain))) {
      await configureS3(connection, process.env);
    }

    await buildChainPrototype({ connection, chain });

    logLine("flow prototype ready", {
      chain,
      database: databasePath,
    });
  } finally {
    connection.closeSync();
  }
};

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logLine("build flow prototype failed", { error: message });
  process.exitCode = 1;
});
