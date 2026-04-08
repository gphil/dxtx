import { createFileStoreDest, getCacheDest, listTransferChunks } from "./cache.js";
import { resolveSqdUrl, supportedChains } from "./chains.js";
import { logLine } from "./log.js";
import type { Chain } from "./types.js";

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

const transferDest = (chain: Chain) => `${getCacheDest(chain).replace(/\/+$/, "")}/erc20-transfers`;

const parseHeight = (value: string, label: string) => {
  const height = Number.parseInt(value, 10);

  if (!Number.isFinite(height)) {
    throw new Error(`invalid ${label}: ${value}`);
  }

  return height;
};

const readStatusHeight = async (chain: Chain) => {
  const dest = createFileStoreDest(transferDest(chain));

  if (!(await dest.exists("status.txt"))) {
    return undefined;
  }

  const [heightText] = (await dest.readFile("status.txt")).trim().split(/\r?\n/);
  return heightText ? parseHeight(heightText, `status height for ${chain}`) : undefined;
};

const readArchiveHeight = async (chain: Chain) => {
  const sqdUrl = resolveSqdUrl(chain);
  const response = await fetch(`${sqdUrl}/height`);

  if (!response.ok) {
    throw new Error(`SQD error for ${chain}: ${response.status} ${response.statusText}`);
  }

  return parseHeight(await response.text(), `archive height for ${chain}`);
};

const summarizeChain = async (chain: Chain) => {
  const chunks = await listTransferChunks({ cacheDest: getCacheDest(chain) });
  const latestChunk = chunks.at(-1);
  const statusHeight = await readStatusHeight(chain);
  const archiveHeight = await readArchiveHeight(chain);
  const publishedHeight = statusHeight ?? latestChunk?.toBlock;
  const lagBlocks = publishedHeight === undefined ? archiveHeight + 1 : Math.max(0, archiveHeight - publishedHeight);

  logLine("checked transfer freshness", {
    chain,
    chunks: chunks.length,
    status_height: statusHeight,
    latest_chunk_to_block: latestChunk?.toBlock,
    archive_height: archiveHeight,
    lag_blocks: lagBlocks,
  });
};

const main = async () => {
  for (const chain of selectedChains(process.argv.slice(2))) {
    await summarizeChain(chain);
  }
};

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logLine("check freshness failed", { error: message });
  process.exitCode = 1;
});
