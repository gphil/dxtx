import { config as loadEnv } from "dotenv";
import { getCacheDest, listTransferChunks } from "../cache.js";
import { resolveRpcUrl, resolveSqdUrl, supportedChains } from "../chains.js";
import { findBlockByTimestamp } from "../evm.js";
import { logLine } from "../log.js";
import { publishTransfersWithProcessor } from "../processor-cache.js";
import { loadDxTokenListTargets, defaultDxTokenListRoot } from "../token-list.js";
import { publishTokenManifestRows } from "./manifest.js";
import { resolveTokenMetadata } from "./metadata.js";
import type { Chain, TokenTarget } from "../types.js";
import type { TransferFilterMode } from "../processor-cache.js";

loadEnv({ quiet: true });

const defaultChunkSizeMb = Number.parseInt(process.env.CACHE_CHUNK_SIZE_MB || "64", 10);

const normalizeEnvName = (chain: Chain) => chain.toUpperCase();

const envValue = (baseKey: string, chain: Chain) =>
  process.env[`${baseKey}_${normalizeEnvName(chain)}`] ?? process.env[baseKey];

const parseNumber = (value: string | undefined, key: string) => {
  if (value === undefined) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);

  if (!Number.isFinite(parsed)) {
    throw new Error(`invalid ${key}: ${value}`);
  }

  return parsed;
};

const parseDate = (value: string | undefined, key: string) => {
  if (value === undefined) {
    return undefined;
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`invalid ${key}: ${value}; expected YYYY-MM-DD`);
  }

  const timestamp = Date.parse(`${value}T00:00:00Z`);

  if (!Number.isFinite(timestamp)) {
    throw new Error(`invalid ${key}: ${value}`);
  }

  return Math.floor(timestamp / 1_000);
};

const resolveFromBlock = async ({
  chain,
  latestHeight,
  rpcUrl,
}: {
  chain: Chain;
  latestHeight: number;
  rpcUrl: string;
}) => {
  const explicitFromBlock = parseNumber(envValue("CACHE_FROM_BLOCK", chain), "CACHE_FROM_BLOCK");

  if (explicitFromBlock !== undefined) {
    return explicitFromBlock;
  }

  const sinceTimestamp = parseDate(envValue("CACHE_FROM_DATE", chain), "CACHE_FROM_DATE");

  if (sinceTimestamp === undefined) {
    throw new Error(`missing CACHE_FROM_DATE or CACHE_FROM_BLOCK for ${chain}`);
  }

  return findBlockByTimestamp({
    targetTimestamp: sinceTimestamp,
    latestBlock: latestHeight,
    rpcUrl,
  });
};

const resolveToBlock = (chain: Chain, latestHeight: number) =>
  parseNumber(envValue("CACHE_TO_BLOCK", chain), "CACHE_TO_BLOCK") ?? latestHeight;

const resolveChunkSizeMb = (chain: Chain) =>
  parseNumber(envValue("CACHE_CHUNK_SIZE_MB", chain), "CACHE_CHUNK_SIZE_MB") ?? defaultChunkSizeMb;

const resolveTargetLimit = (chain: Chain) =>
  parseNumber(envValue("CACHE_TARGET_LIMIT", chain), "CACHE_TARGET_LIMIT");

const resolveTransferFilterMode = (chain: Chain): TransferFilterMode => {
  const value = envValue("CACHE_LOG_FILTER_MODE", chain);

  if (value === undefined || value === "address") {
    return "address";
  }

  if (value === "topic0") {
    return "topic0";
  }

  throw new Error(`invalid CACHE_LOG_FILTER_MODE: ${value}; expected address or topic0`);
};

const resolveDxTokenListRoot = () => process.env.DX_TOKEN_LIST_ROOT || defaultDxTokenListRoot;

const startBlockFromCache = async ({
  cacheDest,
  fromBlock,
}: {
  cacheDest: string;
  fromBlock: number;
}) => {
  const latestChunk = (await listTransferChunks({ cacheDest })).at(-1);
  return Math.max(fromBlock, latestChunk === undefined ? fromBlock : latestChunk.toBlock + 1);
};

export const loadPublishTargetsByChain = async (chains: Chain[]) => {
  const targets = await loadDxTokenListTargets({
    root: resolveDxTokenListRoot(),
    chains,
  });

  return chains.reduce(
    (targetsByChain, chain) =>
      targetsByChain.set(
        chain,
        targets
          .filter((target): target is TokenTarget => target.chain === chain)
          .slice(0, resolveTargetLimit(chain) ?? Number.POSITIVE_INFINITY),
      ),
    new Map<Chain, TokenTarget[]>(),
  );
};

export const runChainPublish = async (chain: Chain) => {
  const sqdUrl = resolveSqdUrl(chain);
  const rpcUrl = resolveRpcUrl(chain);
  const cacheDest = getCacheDest(chain);
  const latestHeightText = await fetch(`${sqdUrl}/height`).then(async (response) => {
    if (!response.ok) {
      throw new Error(`SQD error: ${response.status} ${response.statusText}`);
    }

    return response.text();
  });
  const latestHeight = Number.parseInt(latestHeightText, 10);
  const fromBlock = await resolveFromBlock({
    chain,
    latestHeight,
    rpcUrl,
  });
  const toBlock = resolveToBlock(chain, latestHeight);
  const startBlock = await startBlockFromCache({
    cacheDest,
    fromBlock,
  });
  const limitedTargets = (await loadPublishTargetsByChain([chain])).get(chain) ?? [];
  const targetFileMb = resolveChunkSizeMb(chain);
  const filterMode = resolveTransferFilterMode(chain);

  if (limitedTargets.length === 0) {
    logLine("skipped chain with no token targets", {
      chain,
    });
    return;
  }

  if (startBlock > toBlock) {
    logLine("skipped chain already caught up", {
      chain,
      next_block: startBlock,
      to_block: toBlock,
    });
    return;
  }

  const tokensByAddress = await resolveTokenMetadata({
    chain,
    targets: limitedTargets,
    rpcUrl,
  });

  await publishTokenManifestRows({
    chain,
    cacheDest,
    targets: limitedTargets,
    tokensByAddress,
  });

  logLine("starting chain publish", {
    chain,
    from_block: startBlock,
    to_block: toBlock,
    targets: limitedTargets.length,
    resolved_tokens: tokensByAddress.size,
    target_file_mb: targetFileMb,
    filter_mode: filterMode,
  });

  await publishTransfersWithProcessor({
    chain,
    sqdUrl,
    rpcUrl,
    fromBlock: startBlock,
    toBlock,
    tokenAddresses: limitedTargets.map((target) => target.address),
    tokensByAddress,
    cacheDest,
    targetFileMb,
    filterMode,
  });
};

export const selectedChains = () => {
  const value = process.env.CACHE_CHAINS;

  if (!value) {
    return supportedChains;
  }

  const chains = value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry): entry is Chain => supportedChains.includes(entry as Chain));

  return chains.length > 0 ? chains : supportedChains;
};
