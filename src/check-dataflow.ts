import { DuckDBInstance, type DuckDBConnection } from "@duckdb/node-api";
import { getCacheDest, isS3Uri, listTransferChunks } from "./cache.js";
import { supportedChains } from "./chains.js";
import { escapeSqlString } from "./format.js";
import { logLine } from "./log.js";
import type { Chain } from "./types.js";

type TransferSummary = {
  transfer_rows: number;
  tokens_with_transfers: number;
};

type TransferSample = {
  block_number: bigint | number | string;
  transaction_hash: string;
  token_address: string;
  amount_text: string;
  from_address: string;
  to_address: string;
};

const assertEnv = (value: string | undefined, key: string) => {
  if (!value) {
    throw new Error(`${key} is required when CACHE_DEST points at s3://...`);
  }

  return value;
};

const splitChainArgs = (values: string[]) =>
  values.flatMap((value) => value.split(",")).map((value) => value.trim()).filter(Boolean);

const isChain = (value: string): value is Chain => supportedChains.includes(value as Chain);

const selectedChains = (args: string[]): Chain[] => {
  const requested = splitChainArgs(args);

  if (requested.length === 0) {
    const configured = process.env.CACHE_CHAINS;
    return configured ? selectedChains([configured]) : supportedChains;
  }

  const chains = [...new Set(requested.filter(isChain))];

  if (chains.length === 0) {
    throw new Error(`no supported chains selected; expected one of ${supportedChains.join(",")}`);
  }

  return chains;
};

const createConnection = async () => {
  const instance = await DuckDBInstance.create(":memory:");
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

const latestTransferSample = async ({
  connection,
  chunkUri,
}: {
  connection: DuckDBConnection;
  chunkUri: string;
}) => {
  const result = await rows<TransferSample>(
    connection,
    `
    select
      block_number,
      transaction_hash,
      token_address,
      amount_text,
      from_address,
      to_address
    from read_parquet('${escapeSqlString(chunkUri)}')
    order by block_number desc, transaction_index desc, log_index desc
    limit 1
    `,
  );

  if (result.length === 0) {
    throw new Error(`no transfer rows found in ${chunkUri}`);
  }

  const [sample] = result;

  if (!sample) {
    throw new Error(`no transfer rows found in ${chunkUri}`);
  }

  return sample;
};

const transferSummary = async ({
  connection,
  chunkUris,
}: {
  connection: DuckDBConnection;
  chunkUris: string[];
}) => {
  const result = await rows<TransferSummary>(
    connection,
    `
    select
      count(*) as transfer_rows,
      count(distinct token_address) as tokens_with_transfers
    from read_parquet(${sqlStringList(chunkUris)})
    `,
  );

  if (result.length === 0) {
    throw new Error("no transfer summary returned");
  }

  const [summary] = result;

  if (!summary) {
    throw new Error("no transfer summary returned");
  }

  return summary;
};

const summarizeChain = async ({
  connection,
  chain,
}: {
  connection: DuckDBConnection;
  chain: Chain;
}) => {
  const cacheDest = getCacheDest(chain);
  const chunks = await listTransferChunks({ cacheDest });

  if (chunks.length === 0) {
    logLine("checked transfer data", {
      chain,
      chunks: 0,
      transfer_rows: 0,
      tokens_with_transfers: 0,
    });
    return;
  }

  const chunkUris = chunks.map((chunk) => chunk.cacheUri);
  const summary = await transferSummary({ connection, chunkUris });
  const latestChunkUri = chunkUris[chunks.length - 1];

  if (!latestChunkUri) {
    throw new Error(`missing latest transfer chunk for ${chain}`);
  }

  const sample = await latestTransferSample({ connection, chunkUri: latestChunkUri });

  logLine("checked transfer data", {
    chain,
    chunks: chunks.length,
    transfer_rows: summary.transfer_rows,
    tokens_with_transfers: summary.tokens_with_transfers,
    sample_block: String(sample.block_number),
    sample_tx: sample.transaction_hash,
    sample_token: sample.token_address,
    sample_amount: sample.amount_text,
    sample_from: sample.from_address,
    sample_to: sample.to_address,
  });
};

const main = async () => {
  const chains = selectedChains(process.argv.slice(2));
  const firstChain = chains[0];

  if (!firstChain) {
    throw new Error("no chains selected");
  }

  const connection = await createConnection();

  try {
    if (isS3Uri(getCacheDest(firstChain))) {
      await configureS3(connection, process.env);
    }

    for (const chain of chains) {
      await summarizeChain({ connection, chain });
    }
  } finally {
    connection.closeSync();
  }
};

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logLine("check dataflow failed", { error: message });
  process.exitCode = 1;
});
