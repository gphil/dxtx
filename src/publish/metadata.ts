import { readFile } from "node:fs/promises";
import { fetchTokenMetadataBatch } from "../evm.js";
import { loadDuneTokenMetadata } from "../dune-tokens.js";
import { logLine } from "../log.js";
import { normalizeAddress } from "../format.js";
import type { Chain, TokenMetadata, TokenTarget } from "../types.js";

const defaultRpcBatchSize = 25;
let sharedMetadataPromise: Promise<Map<string, TokenMetadata> | null> | null = null;

const chunkValues = <T>(values: T[], size: number) =>
  Array.from({ length: Math.ceil(values.length / size) }, (_, index) =>
    values.slice(index * size, (index + 1) * size),
  );

const sharedMetadataPath = () => process.env.DXTX_SHARED_METADATA_PATH;

const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : String(error);

const errorCode = (error: unknown) =>
  error && typeof error === "object" && "code" in error ? String((error as { code: unknown }).code) : undefined;

const loadSharedMetadata = async () => {
  const path = sharedMetadataPath();

  if (!path) {
    return null;
  }

  sharedMetadataPromise ??= readFile(path, "utf8")
    .then((value) => JSON.parse(value) as TokenMetadata[])
    .then((tokens) => new Map(tokens.map((token) => [`${token.chain}:${token.address}`, token] as const)))
    .catch((error) => {
      if (errorCode(error) !== "ENOENT") {
        throw error;
      }

      logLine("skipped missing shared token metadata", {
        path,
        error: errorMessage(error),
      });

      return null;
    });

  return sharedMetadataPromise;
};

export const resolveTokenMetadata = async ({
  chain,
  targets,
  rpcUrl,
  rpcBatchSize = defaultRpcBatchSize,
}: {
  chain: Chain;
  targets: TokenTarget[];
  rpcUrl: string;
  rpcBatchSize?: number;
}) => {
  const targetAddresses = [...new Set(targets.map((target) => normalizeAddress(target.address)))];
  const sharedMetadata = await loadSharedMetadata();
  const sharedTokens = targetAddresses.flatMap((address) => {
    const token = sharedMetadata?.get(`${chain}:${address}`);
    return token ? [token] : [];
  });
  const duneTokens =
    sharedMetadata === null
      ? await loadDuneTokenMetadata({
          chain,
          addresses: targetAddresses,
        }).catch((error) => {
          logLine("skipped dune chain token metadata", {
            chain,
            error: error instanceof Error ? error.message : String(error),
          });
          return [];
        })
      : sharedTokens;
  const duneMap = new Map(duneTokens.map((token) => [token.address, token] as const));
  const missingAddresses = targetAddresses.filter((address) => !duneMap.has(address));

  logLine("resolving chain token metadata", {
    chain,
    targets: targetAddresses.length,
    dune_resolved: duneMap.size,
    missing: missingAddresses.length,
  });

  const rpcResults = await chunkValues(missingAddresses, rpcBatchSize).reduce(
    async (allResultsPromise, addresses) => {
      const allResults = await allResultsPromise;
      const startedAt = Date.now();
      const results = await fetchTokenMetadataBatch({
        chain,
        addresses,
        rpcUrl,
      });

      logLine("resolved rpc chain token metadata batch", {
        chain,
        requested: addresses.length,
        resolved: results.filter((result) => result.token !== null).length,
        failures: results.filter((result) => result.token === null).length,
        duration_ms: Date.now() - startedAt,
      });

      return [...allResults, ...results];
    },
    Promise.resolve([] as Awaited<ReturnType<typeof fetchTokenMetadataBatch>>),
  );

  const rpcFailures = rpcResults.filter((result) => result.token === null);

  if (rpcFailures.length > 0) {
    logLine("skipped token metadata batch entries", {
      chain,
      failures: rpcFailures.length,
      first_token_address: rpcFailures[0]?.address,
      first_error: rpcFailures[0]?.error ?? undefined,
    });
  }

  return new Map(
    [...duneTokens, ...rpcResults.flatMap((result) => (result.token ? [result.token] : []))].map(
      (token) => [token.address, token satisfies TokenMetadata] as const,
    ),
  );
};
