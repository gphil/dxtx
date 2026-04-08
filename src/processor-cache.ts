import { Database } from "@subsquid/file-store";
import { Column, Table, Types } from "@subsquid/file-store-parquet";
import { EvmBatchProcessor } from "@subsquid/evm-processor";
import { createFileStoreDest } from "./cache.js";
import {
  bigIntHexToDecimal,
  formatUnitsText,
  safeNumberFromText,
} from "./format.js";
import { logLine } from "./log.js";
import type { Chain, TokenMetadata } from "./types.js";

const transferTopic =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const defaultParquetRowGroupSize = 32 * 1024 * 1024;
const defaultParquetPageSize = 64 * 1024;
const lowerAddress = (value: string) => value.toLowerCase();
const lowerOptionalAddress = (value: string | undefined) => (value ? value.toLowerCase() : null);
const decodeTopicAddress = (topic: string) => `0x${topic.slice(topic.length - 40).toLowerCase()}`;
export type TransferFilterMode = "address" | "topic0";

type PreparedToken = {
  address: string;
  name: string;
  symbol: string;
  decimals: number;
};

type TransferParquetRow = {
  chain: Chain;
  token_address: string;
  token_name: string;
  token_symbol: string;
  token_decimals: number;
  block_number: bigint;
  block_hash: string;
  block_timestamp: bigint;
  transaction_hash: string;
  transaction_index: number;
  log_index: number;
  tx_from: string | null;
  tx_to: string | null;
  tx_sighash: string | null;
  from_address: string;
  to_address: string;
  amount_raw: string;
  amount_text: string;
  amount_double: number | null;
  source: string;
};

const createTransferTable = () =>
  new Table(
    "transfers.parquet",
    {
      chain: Column(Types.String()),
      token_address: Column(Types.String()),
      token_name: Column(Types.String()),
      token_symbol: Column(Types.String()),
      token_decimals: Column(Types.Int32()),
      block_number: Column(Types.Int64()),
      block_hash: Column(Types.String()),
      block_timestamp: Column(Types.Int64()),
      transaction_hash: Column(Types.String()),
      transaction_index: Column(Types.Int32()),
      log_index: Column(Types.Int32()),
      tx_from: Column(Types.String(), { nullable: true }),
      tx_to: Column(Types.String(), { nullable: true }),
      tx_sighash: Column(Types.String(), { nullable: true }),
      from_address: Column(Types.String()),
      to_address: Column(Types.String()),
      amount_raw: Column(Types.String()),
      amount_text: Column(Types.String()),
      amount_double: Column(Types.Double(), { nullable: true }),
      source: Column(Types.String()),
    },
    {
      compression: "GZIP",
      rowGroupSize: defaultParquetRowGroupSize,
      pageSize: defaultParquetPageSize,
    },
  );

const createProcessor = ({
  sqdUrl,
  rpcUrl,
  fromBlock,
  toBlock,
  tokenAddresses,
  filterMode,
}: {
  sqdUrl: string;
  rpcUrl: string;
  fromBlock: number;
  toBlock: number;
  tokenAddresses: string[];
  filterMode: TransferFilterMode;
}) =>
  new EvmBatchProcessor()
    .setGateway(sqdUrl)
    .setRpcEndpoint({
      url: rpcUrl,
      maxBatchCallSize: 100,
      requestTimeout: 60_000,
      rateLimit: 25,
    })
    .setRpcDataIngestionSettings({ disabled: true })
    .setFinalityConfirmation(75)
    .setFields({
      transaction: {
        from: true,
        to: true,
        hash: true,
        sighash: true,
      },
      log: {
        address: true,
        topics: true,
        data: true,
      },
    })
    .setBlockRange({
      from: fromBlock,
      to: toBlock,
    })
    .addLog({
      ...(filterMode === "address" ? { address: tokenAddresses } : {}),
      topic0: [transferTopic],
      transaction: true,
      range: {
        from: fromBlock,
        to: toBlock,
      },
    });

const prepareTokensByAddress = (tokensByAddress: Map<string, TokenMetadata>) =>
  new Map<string, PreparedToken>(
    [...tokensByAddress.entries()].map(([address, token]) => [
      lowerAddress(address),
      {
        address: lowerAddress(address),
        name: token.name,
        symbol: token.symbol,
        decimals: token.decimals,
      },
    ]),
  );

export const publishTransfersWithProcessor = ({
  chain,
  sqdUrl,
  rpcUrl,
  fromBlock,
  toBlock,
  tokenAddresses,
  tokensByAddress,
  cacheDest,
  targetFileMb,
  filterMode,
}: {
  chain: Chain;
  sqdUrl: string;
  rpcUrl: string;
  fromBlock: number;
  toBlock: number;
  tokenAddresses: string[];
  tokensByAddress: Map<string, TokenMetadata>;
  cacheDest: string;
  targetFileMb: number;
  filterMode: TransferFilterMode;
}) => {
  const transferDest = `${cacheDest.replace(/\/+$/, "")}/erc20-transfers`;
  const preparedTokensByAddress = prepareTokensByAddress(tokensByAddress);
  const processor = createProcessor({
    sqdUrl,
    rpcUrl,
    fromBlock,
    toBlock,
    tokenAddresses,
    filterMode,
  });
  const db = new Database({
    tables: {
      transfers: createTransferTable(),
    },
    dest: createFileStoreDest(transferDest),
    chunkSizeMb: targetFileMb,
  });

  let lastBatchAt = Date.now();

  return processor.run(db, async (ctx) => {
    const batchStartedAt = lastBatchAt;
    const rows: TransferParquetRow[] = [];
    const transformStartedAt = Date.now();

    for (const block of ctx.blocks) {
      for (const log of block.logs) {
        const token = preparedTokensByAddress.get(lowerAddress(log.address));

        if (!token) {
          continue;
        }

        const transaction = log.getTransaction();
        const amountRaw = bigIntHexToDecimal(log.data);
        const amountText = formatUnitsText(amountRaw, token.decimals);

        rows.push({
          chain,
          token_address: token.address,
          token_name: token.name,
          token_symbol: token.symbol,
          token_decimals: token.decimals,
          block_number: BigInt(log.block.height),
          block_hash: log.block.hash,
          block_timestamp: BigInt(log.block.timestamp),
          transaction_hash: transaction.hash,
          transaction_index: log.transactionIndex,
          log_index: log.logIndex,
          tx_from: lowerOptionalAddress(transaction.from),
          tx_to: lowerOptionalAddress(transaction.to),
          tx_sighash: transaction.sighash || null,
          from_address: decodeTopicAddress(log.topics[1] ?? ""),
          to_address: decodeTopicAddress(log.topics[2] ?? ""),
          amount_raw: amountRaw,
          amount_text: amountText,
          amount_double: safeNumberFromText(amountText),
          source: "sqd-processor",
        });
      }
    }
    const transformFinishedAt = Date.now();
    const firstHeight = ctx.blocks[0]?.header.height;
    const lastHeight = ctx.blocks.at(-1)?.header.height;
    const blocks =
      firstHeight === undefined || lastHeight === undefined ? undefined : lastHeight - firstHeight + 1;

    const writeStartedAt = Date.now();
    ctx.store.transfers.writeMany(rows);
    const writeFinishedAt = Date.now();

    const finishedAt = Date.now();
    const batchMs = finishedAt - batchStartedAt;
    lastBatchAt = finishedAt;
    const batchSeconds = batchMs > 0 ? batchMs / 1_000 : undefined;
    const transformMs = transformFinishedAt - transformStartedAt;
    const writeMs = writeFinishedAt - writeStartedAt;
    const waitMs = Math.max(0, batchMs - transformMs - writeMs);

    logLine("processed processor batch", {
      chain,
      batch: firstHeight !== undefined && lastHeight !== undefined ? `${firstHeight}-${lastHeight}` : undefined,
      blocks,
      rows: rows.length,
      ms: batchMs,
      transform_ms: transformMs,
      write_ms: writeMs,
      wait_ms: waitMs,
      filter_mode: filterMode,
      blk_s:
        batchSeconds !== undefined && blocks !== undefined ? Math.round((blocks / batchSeconds) * 100) / 100 : undefined,
      row_s: batchSeconds !== undefined ? Math.round((rows.length / batchSeconds) * 100) / 100 : undefined,
    });

    if (ctx.isHead || (lastHeight !== undefined && lastHeight >= toBlock)) {
      ctx.store.setForceFlush(true);
    }
  });
};
