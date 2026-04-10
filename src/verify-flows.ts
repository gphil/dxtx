import { DuckDBInstance, type DuckDBConnection } from "@duckdb/node-api";
import { getCacheDest, isS3Uri, listTransferChunks } from "./cache.js";
import { resolveRpcUrl, resolveSqdUrl, supportedChains } from "./chains.js";
import { fetchTransactionReceipts, findBlockByTimestamp, type RpcReceipt } from "./evm.js";
import { escapeSqlString, formatUnitsText, normalizeAddress } from "./format.js";
import type { Chain } from "./types.js";

const transferTopic =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

const assertEnv = (value: string | undefined, key: string) => {
  if (!value) {
    throw new Error(`${key} is required when CACHE_DEST points at s3://...`);
  }

  return value;
};

const splitArgs = (values: string[]) =>
  values.flatMap((value) => value.split(",")).map((value) => value.trim()).filter(Boolean);

const isChain = (value: string): value is Chain => supportedChains.includes(value as Chain);

const parseDay = (value: string) => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`invalid day: ${value}; expected YYYY-MM-DD`);
  }

  return value;
};

const nextDay = (value: string) =>
  new Date(Date.parse(`${value}T00:00:00Z`) + 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

const dayTimestamp = (value: string) => Math.floor(Date.parse(`${value}T00:00:00Z`) / 1_000);

const sqlStringList = (values: string[]) =>
  `[${values.map((value) => `'${escapeSqlString(value)}'`).join(", ")}]`;

const outputPath = (chain: Chain) =>
  process.env.ANALYTICS_DB_PATH || `./analytics/${chain}-flows.duckdb`;

const createConnection = async (databasePath: string) => {
  const instance = await DuckDBInstance.create(databasePath);
  return instance.connect();
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

  const run = async (sql: string) => {
    await connection.run(sql);
  };

  await run("INSTALL httpfs; LOAD httpfs");
  await run(`SET s3_region='${escapeSqlString(region)}'`);
  await run(`SET s3_endpoint='${escapeSqlString(endpoint)}'`);
  await run(`SET s3_access_key_id='${escapeSqlString(accessKeyId)}'`);
  await run(`SET s3_secret_access_key='${escapeSqlString(secretAccessKey)}'`);
  await run(`SET s3_url_style='path'`);
  await run(`SET s3_use_ssl=${useSsl}`);

  if (sessionToken) {
    await run(`SET s3_session_token='${escapeSqlString(sessionToken)}'`);
  }
};

const parseArgs = (args: string[]) => {
  const values = splitArgs(args);
  const [chainValue, tokenValue, dayValue, ...addressValues] = values;

  if (!chainValue || !isChain(chainValue) || !tokenValue || !dayValue || addressValues.length === 0) {
    throw new Error(
      "usage: npm run verify:flows -- <chain> <token-symbol|coingecko-id|address> <YYYY-MM-DD> <address> [address...]",
    );
  }

  return {
    chain: chainValue,
    token: tokenValue,
    day: parseDay(dayValue),
    addresses: addressValues.map(normalizeAddress),
  };
};

type TokenRow = {
  chain: string;
  address: string;
  token_name: string;
  token_symbol: string;
  token_decimals: number;
  coingecko_id: string | null;
};

type AnalyticsFlowRow = {
  address: string;
  sent_transfer_count: bigint | number;
  received_transfer_count: bigint | number;
  total_transfer_count: bigint | number;
  sent_amount_native_sum: number;
  received_amount_native_sum: number;
  gross_amount_native_sum: number;
  net_amount_native_sum: number;
};

type RawTransferRow = {
  block_number: bigint | number;
  transaction_hash: string;
  log_index: number;
  from_address: string;
  to_address: string;
  amount_raw: string;
  amount_text: string;
};

type ExactAggregate = {
  sentTransferCount: number;
  receivedTransferCount: number;
  totalTransferCount: number;
  sentAmountRaw: bigint;
  receivedAmountRaw: bigint;
  grossAmountRaw: bigint;
  netAmountRaw: bigint;
};

type RpcVerificationRow = {
  transfer: RawTransferRow;
  ok: boolean;
  rpc_from_address?: string;
  rpc_to_address?: string;
  rpc_amount_raw?: string;
  error?: string;
};

const tokenLookupSql = (token: string) => {
  const value = escapeSqlString(token.toLowerCase());
  const addressMatch = token.startsWith("0x") ? `lower(address) = '${value}'` : "false";

  return `
    select
      chain,
      address,
      token_name,
      token_symbol,
      token_decimals,
      coingecko_id
    from token_manifest
    where ${addressMatch}
       or lower(token_symbol) = '${value}'
       or lower(token_name) = '${value}'
       or lower(coingecko_id) = '${value}'
    order by
      case when lower(address) = '${value}' then 0 else 1 end,
      address asc
  `;
};

const analyticsFlowSql = ({
  tokenAddress,
  day,
  addresses,
}: {
  tokenAddress: string;
  day: string;
  addresses: string[];
}) => `
  select
    address,
    sent_transfer_count,
    received_transfer_count,
    total_transfer_count,
    sent_amount_native_sum,
    received_amount_native_sum,
    gross_amount_native_sum,
    net_amount_native_sum
  from token_daily_address_flows
  where token_address = '${escapeSqlString(tokenAddress)}'
    and day = date '${escapeSqlString(day)}'
    and lower(address) in (${addresses.map((value) => `'${escapeSqlString(value)}'`).join(", ")})
  order by address asc
`;

const rawTransferSql = ({
  chunkUris,
  tokenAddress,
  fromBlock,
  toBlock,
  addresses,
}: {
  chunkUris: string[];
  tokenAddress: string;
  fromBlock: number;
  toBlock: number;
  addresses: string[];
}) => `
  select
    block_number,
    transaction_hash,
    log_index,
    lower(from_address) as from_address,
    lower(to_address) as to_address,
    amount_raw,
    amount_text
  from read_parquet(${sqlStringList(chunkUris)})
  where lower(token_address) = '${escapeSqlString(tokenAddress)}'
    and block_number >= ${fromBlock}
    and block_number <= ${toBlock}
    and (
      lower(from_address) in (${addresses.map((value) => `'${escapeSqlString(value)}'`).join(", ")})
      or lower(to_address) in (${addresses.map((value) => `'${escapeSqlString(value)}'`).join(", ")})
    )
  order by block_number asc, log_index asc
`;

const toCount = (value: bigint | number | undefined) => Number(value ?? 0);

const exactAggregate = (address: string, transfers: RawTransferRow[]): ExactAggregate =>
  transfers.reduce<ExactAggregate>(
    (current, transfer) => {
      const amountRaw = BigInt(transfer.amount_raw);
      const sentTransferCount = transfer.from_address === address ? 1 : 0;
      const receivedTransferCount = transfer.to_address === address ? 1 : 0;
      const sentAmountRaw = transfer.from_address === address ? amountRaw : 0n;
      const receivedAmountRaw = transfer.to_address === address ? amountRaw : 0n;

      return {
        sentTransferCount: current.sentTransferCount + sentTransferCount,
        receivedTransferCount: current.receivedTransferCount + receivedTransferCount,
        totalTransferCount: current.totalTransferCount + sentTransferCount + receivedTransferCount,
        sentAmountRaw: current.sentAmountRaw + sentAmountRaw,
        receivedAmountRaw: current.receivedAmountRaw + receivedAmountRaw,
        grossAmountRaw: current.grossAmountRaw + sentAmountRaw + receivedAmountRaw,
        netAmountRaw: current.netAmountRaw + receivedAmountRaw - sentAmountRaw,
      };
    },
    {
      sentTransferCount: 0,
      receivedTransferCount: 0,
      totalTransferCount: 0,
      sentAmountRaw: 0n,
      receivedAmountRaw: 0n,
      grossAmountRaw: 0n,
      netAmountRaw: 0n,
    },
  );

const decodeTopicAddress = (topic: string) => `0x${topic.slice(topic.length - 40).toLowerCase()}`;
const toNativeNumber = (amountRaw: bigint, decimals: number) => Number(formatUnitsText(amountRaw.toString(), decimals));
const amountTolerance = (expected: number, decimals: number) =>
  Math.max(10 ** -Math.min(decimals, 6), Math.abs(expected) * 1e-12);

const analyticsMatchesExact = ({
  analytics,
  exact,
  decimals,
}: {
  analytics: AnalyticsFlowRow | undefined;
  exact: ExactAggregate;
  decimals: number;
}) => {
  if (!analytics) {
    return false;
  }

  const expectedSent = toNativeNumber(exact.sentAmountRaw, decimals);
  const expectedReceived = toNativeNumber(exact.receivedAmountRaw, decimals);
  const expectedGross = toNativeNumber(exact.grossAmountRaw, decimals);
  const expectedNet = toNativeNumber(exact.netAmountRaw, decimals);

  const matchesAmount = (actual: number, expected: number) =>
    Math.abs(actual - expected) <= amountTolerance(expected, decimals);

  return (
    toCount(analytics.sent_transfer_count) === exact.sentTransferCount &&
    toCount(analytics.received_transfer_count) === exact.receivedTransferCount &&
    toCount(analytics.total_transfer_count) === exact.totalTransferCount &&
    matchesAmount(analytics.sent_amount_native_sum, expectedSent) &&
    matchesAmount(analytics.received_amount_native_sum, expectedReceived) &&
    matchesAmount(analytics.gross_amount_native_sum, expectedGross) &&
    matchesAmount(analytics.net_amount_native_sum, expectedNet)
  );
};

const resolveBlockRange = async ({
  chain,
  day,
}: {
  chain: Chain;
  day: string;
}) => {
  const rpcUrl = resolveRpcUrl(chain);
  const sqdUrl = resolveSqdUrl(chain);
  const latestBlock = Number.parseInt(
    await fetch(`${sqdUrl}/height`).then(async (response) => {
      if (!response.ok) {
        throw new Error(`SQD error for ${chain}: ${response.status} ${response.statusText}`);
      }

      return response.text();
    }),
    10,
  );

  const fromBlock = await findBlockByTimestamp({
    targetTimestamp: dayTimestamp(day),
    latestBlock,
    rpcUrl,
  });
  const toBlock =
    (
      await findBlockByTimestamp({
        targetTimestamp: dayTimestamp(nextDay(day)),
        latestBlock,
        rpcUrl,
      })
    ) - 1;

  return {
    rpcUrl,
    fromBlock,
    toBlock: Math.max(0, toBlock),
  };
};

const verifyTransfersWithRpc = ({
  tokenAddress,
  transfers,
  receiptsByHash,
}: {
  tokenAddress: string;
  transfers: RawTransferRow[];
  receiptsByHash: Map<string, RpcReceipt>;
}): RpcVerificationRow[] =>
  transfers.map((transfer) => {
    const receipt = receiptsByHash.get(transfer.transaction_hash);

    if (!receipt) {
      return {
        transfer,
        ok: false,
        error: "missing receipt",
      };
    }

    const log = receipt.logs.find(
      (entry) =>
        Number.parseInt(entry.logIndex, 16) === transfer.log_index &&
        entry.address.toLowerCase() === tokenAddress &&
        entry.topics[0]?.toLowerCase() === transferTopic,
    );

    if (!log) {
      return {
        transfer,
        ok: false,
        error: "missing receipt log",
      };
    }

    const fromAddress = decodeTopicAddress(log.topics[1] ?? "");
    const toAddress = decodeTopicAddress(log.topics[2] ?? "");
    const amountRaw = BigInt(log.data === "0x" ? "0x0" : log.data).toString(10);
    const ok =
      fromAddress === transfer.from_address &&
      toAddress === transfer.to_address &&
      amountRaw === transfer.amount_raw;

    return {
      transfer,
      ok,
      rpc_from_address: fromAddress,
      rpc_to_address: toAddress,
      rpc_amount_raw: amountRaw,
      ...(ok
        ? {}
        : {
            error: `rpc mismatch from=${fromAddress} to=${toAddress} amount_raw=${amountRaw}`,
          }),
    };
  });

const formatExactAggregate = (aggregate: ExactAggregate, decimals: number) => ({
  sent_transfer_count: aggregate.sentTransferCount,
  received_transfer_count: aggregate.receivedTransferCount,
  total_transfer_count: aggregate.totalTransferCount,
  sent_amount_native_exact: formatUnitsText(aggregate.sentAmountRaw.toString(), decimals),
  received_amount_native_exact: formatUnitsText(aggregate.receivedAmountRaw.toString(), decimals),
  gross_amount_native_exact: formatUnitsText(aggregate.grossAmountRaw.toString(), decimals),
  net_amount_native_exact: formatUnitsText(aggregate.netAmountRaw.toString(), decimals),
});

const formatAnalyticsAggregate = (analytics: AnalyticsFlowRow | undefined) =>
  analytics
    ? {
        sent_transfer_count: toCount(analytics.sent_transfer_count),
        received_transfer_count: toCount(analytics.received_transfer_count),
        total_transfer_count: toCount(analytics.total_transfer_count),
        sent_amount_native_sum: analytics.sent_amount_native_sum,
        received_amount_native_sum: analytics.received_amount_native_sum,
        gross_amount_native_sum: analytics.gross_amount_native_sum,
        net_amount_native_sum: analytics.net_amount_native_sum,
      }
    : {
        sent_transfer_count: null,
        received_transfer_count: null,
        total_transfer_count: null,
        sent_amount_native_sum: null,
        received_amount_native_sum: null,
        gross_amount_native_sum: null,
        net_amount_native_sum: null,
      };

const printVerification = ({
  token,
  day,
  address,
  analytics,
  exact,
  transfers,
  verification,
}: {
  token: TokenRow;
  day: string;
  address: string;
  analytics: AnalyticsFlowRow | undefined;
  exact: ExactAggregate;
  transfers: RawTransferRow[];
  verification: RpcVerificationRow[];
}) => {
  const analyticsOk = analyticsMatchesExact({
    analytics,
    exact,
    decimals: token.token_decimals,
  });
  const rpcFailures = verification.filter((row) => !row.ok);
  const rpcOk = rpcFailures.length === 0;
  const passed = analyticsOk && rpcOk;
  const status = passed ? "verification passed - rpc matched" : "verification failed";
  const reasons = [analyticsOk ? undefined : "index aggregate mismatch", rpcOk ? undefined : "rpc mismatch"]
    .filter(Boolean)
    .join(", ");

  console.log(
    `${status} chain=${token.chain} token=${token.token_symbol} day=${day} address=${address}` +
      (reasons ? ` reason=${reasons}` : "") +
      ` logs=${transfers.length}`,
  );

  if (passed) {
    return;
  }

  console.log("index aggregate:");
  console.table([formatAnalyticsAggregate(analytics)]);
  console.log("exact aggregate from cached transfers:");
  console.table([formatExactAggregate(exact, token.token_decimals)]);

  if (!rpcOk) {
    console.log("rpc mismatch details:");
    console.table(
      rpcFailures.map(({ transfer, error, rpc_from_address, rpc_to_address, rpc_amount_raw }) => ({
        transaction_hash: transfer.transaction_hash,
        log_index: transfer.log_index,
        index_from_address: transfer.from_address,
        rpc_from_address,
        index_to_address: transfer.to_address,
        rpc_to_address,
        index_amount_text: transfer.amount_text,
        rpc_amount_text:
          rpc_amount_raw === undefined ? undefined : formatUnitsText(rpc_amount_raw, token.token_decimals),
        error,
      })),
    );
  }
};

const main = async () => {
  const { chain, token, day, addresses } = parseArgs(process.argv.slice(2));
  const connection = await createConnection(outputPath(chain));

  try {
    if (isS3Uri(getCacheDest(chain))) {
      await configureS3(connection, process.env);
    }

    const tokenMatches = await rows<TokenRow>(connection, tokenLookupSql(token));

    if (tokenMatches.length !== 1) {
      throw new Error(
        tokenMatches.length === 0
          ? `no token found for '${token}'`
          : `token '${token}' is ambiguous; rerun with address`,
      );
    }

    const [resolvedToken] = tokenMatches;

    if (!resolvedToken) {
      throw new Error(`no token found for '${token}'`);
    }

    const { rpcUrl, fromBlock, toBlock } = await resolveBlockRange({ chain, day });
    const chunkUris = (
      await listTransferChunks({
        cacheDest: getCacheDest(chain),
        fromBlock,
        toBlock,
      })
    ).map((chunk) => chunk.cacheUri);

    const analyticsRows = await rows<AnalyticsFlowRow>(
      connection,
      analyticsFlowSql({
        tokenAddress: resolvedToken.address,
        day,
        addresses,
      }),
    );

    const rawTransfers = await rows<RawTransferRow>(
      connection,
      rawTransferSql({
        chunkUris,
        tokenAddress: resolvedToken.address,
        fromBlock,
        toBlock,
        addresses,
      }),
    );

    const receipts = await fetchTransactionReceipts({
      rpcUrl,
      txHashes: [...new Set(rawTransfers.map((transfer) => transfer.transaction_hash))],
    });
    const receiptsByHash = new Map(receipts.map((receipt) => [receipt.transactionHash, receipt]));

    addresses.forEach((address) => {
      const addressTransfers = rawTransfers.filter(
        (transfer) => transfer.from_address === address || transfer.to_address === address,
      );
      const analytics = analyticsRows.find((row) => row.address === address);
      const exact = exactAggregate(address, addressTransfers);
      const verification = verifyTransfersWithRpc({
        tokenAddress: resolvedToken.address,
        transfers: addressTransfers,
        receiptsByHash,
      });

      printVerification({
        token: resolvedToken,
        day,
        address,
        analytics,
        exact,
        transfers: addressTransfers,
        verification,
      });
    });
  } finally {
    connection.closeSync();
  }
};

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exitCode = 1;
});
