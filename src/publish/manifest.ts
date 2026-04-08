import { mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import {
  DuckDBInstance,
  type DuckDBAppender,
  type DuckDBConnection,
} from "@duckdb/node-api";
import { getTokenManifestPath, isS3Uri, uploadFileToCache } from "../cache.js";
import { escapeSqlString } from "../format.js";
import type { Chain, TokenMetadata, TokenTarget } from "../types.js";

type ManifestRow = {
  chain: Chain;
  address: string;
  targetSource: string;
  coingeckoId: string | null;
  coingeckoName: string | null;
  coingeckoSymbol: string | null;
  tokenName: string | null;
  tokenSymbol: string | null;
  tokenDecimals: number | null;
};

const createConnection = async () => {
  const instance = await DuckDBInstance.create(":memory:");
  return instance.connect();
};

const run = async (connection: DuckDBConnection, sql: string) => {
  await connection.run(sql);
};

const createAppender = (connection: DuckDBConnection) => connection.createAppender("token_manifest");

const appendRow = (appender: DuckDBAppender, row: ManifestRow) => {
  appender.appendVarchar(row.chain);
  appender.appendVarchar(row.address);
  appender.appendVarchar(row.targetSource);
  row.coingeckoId === null ? appender.appendNull() : appender.appendVarchar(row.coingeckoId);
  row.coingeckoName === null ? appender.appendNull() : appender.appendVarchar(row.coingeckoName);
  row.coingeckoSymbol === null
    ? appender.appendNull()
    : appender.appendVarchar(row.coingeckoSymbol);
  row.tokenName === null ? appender.appendNull() : appender.appendVarchar(row.tokenName);
  row.tokenSymbol === null ? appender.appendNull() : appender.appendVarchar(row.tokenSymbol);
  row.tokenDecimals === null ? appender.appendNull() : appender.appendInteger(row.tokenDecimals);
  appender.endRow();
};

const manifestRows = ({
  chain,
  targets,
  tokensByAddress,
}: {
  chain: Chain;
  targets: TokenTarget[];
  tokensByAddress: Map<string, TokenMetadata>;
}) =>
  targets.map((target) => {
    const token = tokensByAddress.get(target.address);

    return {
      chain,
      address: target.address,
      targetSource: target.source,
      coingeckoId: target.coingeckoId ?? null,
      coingeckoName: target.coingeckoName ?? null,
      coingeckoSymbol: target.coingeckoSymbol ?? null,
      tokenName: token?.name ?? null,
      tokenSymbol: token?.symbol ?? null,
      tokenDecimals: token?.decimals ?? null,
    } satisfies ManifestRow;
  });

export const publishTokenManifestRows = async ({
  chain,
  cacheDest,
  targets,
  tokensByAddress,
}: {
  chain: Chain;
  cacheDest: string;
  targets: TokenTarget[];
  tokensByAddress: Map<string, TokenMetadata>;
}) => {
  const cacheUri = getTokenManifestPath({ cacheDest });

  if (!isS3Uri(cacheUri)) {
    await mkdir(dirname(cacheUri), { recursive: true });
  }

  const localStagePath = isS3Uri(cacheUri)
    ? join(tmpdir(), "dxtx-cache-stage", `${chain}-token-manifest.parquet`)
    : cacheUri;

  await mkdir(dirname(localStagePath), { recursive: true });

  const connection = await createConnection();

  try {
    await run(
      connection,
      `
      create table token_manifest (
        chain varchar not null,
        address varchar not null,
        target_source varchar not null,
        coingecko_id varchar,
        coingecko_name varchar,
        coingecko_symbol varchar,
        token_name varchar,
        token_symbol varchar,
        token_decimals integer
      )
      `,
    );

    const appender = await createAppender(connection);
    manifestRows({ chain, targets, tokensByAddress }).forEach((row) => appendRow(appender, row));
    appender.closeSync();

    await run(
      connection,
      `
      copy (
        select *
        from token_manifest
        order by address asc
      ) to '${escapeSqlString(localStagePath)}'
      (format parquet, compression zstd, overwrite_or_ignore true)
      `,
    );
  } finally {
    connection.closeSync();
  }

  if (isS3Uri(cacheUri)) {
    await uploadFileToCache({
      localPath: localStagePath,
      cacheUri,
    });
    await rm(localStagePath, { force: true });
  }
};
