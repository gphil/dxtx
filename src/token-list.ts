import { readFile, readdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { getChainConfig } from "./chains.js";
import { normalizeAddress } from "./format.js";
import type { Chain, TokenTarget } from "./types.js";

const currentDir = dirname(fileURLToPath(import.meta.url));
export const defaultDxTokenListRoot = join(currentDir, "..", "token-list");
const zeroAddress = "0x0000000000000000000000000000000000000000";
const coingeckoPlatformByChain = new Map<Chain, string>([
  ["ethereum", "ethereum"],
  ["base", "base"],
  ["arbitrum", "arbitrum-one"],
  ["optimism", "optimistic-ethereum"],
  ["polygon", "polygon-pos"],
  ["avalanche", "avalanche"],
  ["unichain", "unichain"],
  ["bsc", "binance-smart-chain"],
]);
const chainByCoinGeckoPlatform = new Map(
  [...coingeckoPlatformByChain.entries()].map(([chain, platform]) => [platform, chain] as const),
);
const coingeckoFilePattern = /^coingecko-tokens-.*\.csv$/;

const normalizeLines = (value: string) =>
  value
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

const normalizeKey = (value: string) => value.replace(/^\uFEFF/, "").trim().toLowerCase();

const parseCsvRow = (line: string) => {
  const values: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index] ?? "";
    const next = line[index + 1] ?? "";

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }

      continue;
    }

    if (char === "," && !inQuotes) {
      values.push(current.trim());
      current = "";
      continue;
    }

    current += char;
  }

  return [...values, current.trim()];
};

const readCsvRows = async (filePath: string) => {
  const [headerLine, ...dataLines] = normalizeLines(await readFile(filePath, "utf8"));

  if (!headerLine) {
    return [];
  }

  const headers = parseCsvRow(headerLine).map(normalizeKey);

  return dataLines.map((line) =>
    Object.fromEntries(
      headers.map((header, index) => [header, parseCsvRow(line)[index] ?? ""]),
    ) as Record<string, string>,
  );
};

const readLatestCoinGeckoMetadata = async ({
  root,
  chains,
}: {
  root: string;
  chains: Chain[];
}) => {
  const fileName = (await readdir(root))
    .filter((entry) => coingeckoFilePattern.test(entry))
    .sort()
    .at(-1);

  if (!fileName) {
    return new Map<string, Pick<TokenTarget, "coingeckoId" | "coingeckoName" | "coingeckoSymbol">>();
  }

  return (await readCsvRows(join(root, fileName))).reduce((metadataByAddress, row) => {
    const platform = row.platform?.trim().toLowerCase();
    const chain = platform ? chainByCoinGeckoPlatform.get(platform) : undefined;
    const address = row.contract_address?.trim();

    if (!chain || !chains.includes(chain) || !address || address === zeroAddress) {
      return metadataByAddress;
    }

    try {
      const normalizedAddress = normalizeAddress(address);
      return metadataByAddress.set(`${chain}:${normalizedAddress}`, {
        ...(row.id ? { coingeckoId: row.id } : {}),
        ...(row.name ? { coingeckoName: row.name } : {}),
        ...(row.symbol ? { coingeckoSymbol: row.symbol } : {}),
      });
    } catch {
      return metadataByAddress;
    }
  }, new Map<string, Pick<TokenTarget, "coingeckoId" | "coingeckoName" | "coingeckoSymbol">>());
};

const toTokenTarget = ({
  chain,
  row,
  source,
  fallbackMetadataByAddress,
}: {
  chain: Chain;
  row: Record<string, string>;
  source: string;
  fallbackMetadataByAddress: Map<string, Pick<TokenTarget, "coingeckoId" | "coingeckoName" | "coingeckoSymbol">>;
}) => {
  const addressValue = row.address || row.contract_address;

  if (!addressValue) {
    return null;
  }

  const address = normalizeAddress(addressValue);

  if (address === zeroAddress) {
    return null;
  }

  const directMetadata = {
    ...(row.id ? { coingeckoId: row.id } : {}),
    ...(row.name ? { coingeckoName: row.name } : {}),
    ...(row.symbol ? { coingeckoSymbol: row.symbol } : {}),
  };
  const metadata =
    Object.keys(directMetadata).length > 0
      ? directMetadata
      : (fallbackMetadataByAddress.get(`${chain}:${address}`) ?? {});

  return {
    chain,
    address,
    source,
    ...metadata,
  } satisfies TokenTarget;
};

export const loadDxTokenListTargets = async ({
  root = defaultDxTokenListRoot,
  chains,
  source = "dx-token-list",
}: {
  root?: string;
  chains: Chain[];
  source?: string;
}) => {
  const fallbackMetadataByAddress = await readLatestCoinGeckoMetadata({
    root,
    chains,
  });
  const targets = await Promise.all(
    chains.map(async (chain) => {
      const filePath = join(root, getChainConfig(chain).tokenListFile);
      const rows = await readCsvRows(filePath);

      return [
        ...rows.reduce((targetsByAddress, row) => {
          const target = toTokenTarget({
            chain,
            row,
            source,
            fallbackMetadataByAddress,
          });

          if (!target) {
            return targetsByAddress;
          }

          const current = targetsByAddress.get(target.address);
          const mergedTarget = {
            ...(current ?? {}),
            ...target,
            ...(current?.coingeckoId ? { coingeckoId: current.coingeckoId } : {}),
            ...(current?.coingeckoName ? { coingeckoName: current.coingeckoName } : {}),
            ...(current?.coingeckoSymbol ? { coingeckoSymbol: current.coingeckoSymbol } : {}),
            ...(target.coingeckoId ? { coingeckoId: target.coingeckoId } : {}),
            ...(target.coingeckoName ? { coingeckoName: target.coingeckoName } : {}),
            ...(target.coingeckoSymbol ? { coingeckoSymbol: target.coingeckoSymbol } : {}),
          } satisfies TokenTarget;

          return targetsByAddress.set(target.address, mergedTarget);
        }, new Map<string, TokenTarget>()).values(),
      ];
    }),
  );

  return targets.flat();
};
