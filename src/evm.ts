import { normalizeAddress } from "./format.js";
import type { Chain, TokenMetadata } from "./types.js";

const decimalsSelector = "0x313ce567";
const symbolSelector = "0x95d89b41";
const nameSelector = "0x06fdde03";

type RpcResponse<T> = {
  id: number;
  jsonrpc: "2.0";
  result?: T;
  error?: {
    code: number;
    message: string;
  };
};

type RpcBatchResponse<T> = Array<RpcResponse<T>>;
type RpcRequest = {
  id: number;
  jsonrpc: "2.0";
  method: string;
  params: unknown[];
};

export type RpcLog = {
  address: string;
  topics: string[];
  data: string;
  logIndex: string;
  transactionHash: string;
  transactionIndex: string;
};

export type RpcReceipt = {
  transactionHash: string;
  transactionIndex: string;
  blockNumber: string;
  from: string;
  to?: string | null;
  logs: RpcLog[];
};

type RpcBlock = {
  number: string;
  timestamp: string;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const chunkValues = <T>(values: T[], size: number) =>
  Array.from({ length: Math.ceil(values.length / size) }, (_, index) =>
    values.slice(index * size, (index + 1) * size),
  );

const decodeStringResult = (hexValue: string) => {
  if (hexValue === "0x" || hexValue.length < 2) {
    throw new Error("empty RPC string result");
  }

  const payload = hexValue.slice(2);

  if (payload.length === 64) {
    const text = Buffer.from(payload, "hex")
      .toString("utf8")
      .replace(/\0+$/, "")
      .trim();

    if (text.length > 0) {
      return text;
    }
  }

  const offset = Number.parseInt(payload.slice(0, 64), 16);
  const length = Number.parseInt(payload.slice(offset * 2, offset * 2 + 64), 16);
  const textHex = payload.slice(offset * 2 + 64, offset * 2 + 64 + length * 2);

  return Buffer.from(textHex, "hex").toString("utf8");
};

const decodeIntegerResult = (hexValue: string) => Number.parseInt(hexValue, 16);

const postRpc = async <T>(rpcUrl: string, body: unknown, attempt = 0): Promise<T> => {
  let response: Response;

  try {
    response = await fetch(rpcUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    });
  } catch (error) {
    if (attempt < 4) {
      await sleep(500 * 2 ** attempt);
      return postRpc<T>(rpcUrl, body, attempt + 1);
    }

    throw error;
  }

  if (response.ok) {
    return (await response.json()) as T;
  }

  const shouldRetry = response.status === 429 || response.status >= 500;

  if (shouldRetry && attempt < 4) {
    await sleep(500 * 2 ** attempt);
    return postRpc<T>(rpcUrl, body, attempt + 1);
  }

  throw new Error(`RPC error: ${response.status} ${response.statusText}`);
};

const requireResult = <T>(payload: RpcResponse<T>) => {
  if (payload.error) {
    throw new Error(`RPC error: ${payload.error.message}`);
  }

  if (payload.result === undefined) {
    throw new Error("RPC returned no result");
  }

  return payload.result;
};

const fetchRpcMethod = async <T>(rpcUrl: string, method: string, params: unknown[]) => {
  const payload = await postRpc<RpcResponse<T>>(rpcUrl, {
    id: 1,
    jsonrpc: "2.0",
    method,
    params,
  });

  return requireResult(payload);
};

const fetchRpcBatch = async <T>(rpcUrl: string, requests: RpcRequest[]) => {
  const payload = await postRpc<RpcBatchResponse<T> | RpcResponse<T>>(rpcUrl, requests);

  return Array.isArray(payload) ? payload : [payload];
};

const requireBatchResult = <T>(responseById: Map<number, RpcResponse<T>>, id: number) => {
  const response = responseById.get(id);

  if (!response) {
    throw new Error(`RPC batch returned no response for request ${id}`);
  }

  return requireResult(response);
};

const callToken = (rpcUrl: string, address: string, data: string) =>
  fetchRpcMethod<string>(rpcUrl, "eth_call", [
    {
      to: address,
      data,
    },
    "latest",
  ]);

const toHexBlockTag = (blockNumber: number) => `0x${blockNumber.toString(16)}`;

const parseHexNumber = (value: string) => Number.parseInt(value, 16);

export const probeRpcUrl = async (rpcUrl: string) => {
  await fetchRpcMethod<string>(rpcUrl, "eth_chainId", []);
};

export const fetchBlock = async (blockNumber: number, rpcUrl: string) => {
  const block = await fetchRpcMethod<RpcBlock>(rpcUrl, "eth_getBlockByNumber", [
    toHexBlockTag(blockNumber),
    false,
  ]);

  return {
    number: parseHexNumber(block.number),
    timestamp: parseHexNumber(block.timestamp),
  };
};

export const findBlockByTimestamp = async ({
  targetTimestamp,
  latestBlock,
  rpcUrl,
}: {
  targetTimestamp: number;
  latestBlock: number;
  rpcUrl: string;
}) => {
  let low = 0;
  let high = latestBlock;

  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    const block = await fetchBlock(mid, rpcUrl);

    if (block.timestamp < targetTimestamp) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  return low;
};

export const fetchTransactionReceipts = async ({
  txHashes,
  rpcUrl,
}: {
  txHashes: string[];
  rpcUrl: string;
}) => {
  if (txHashes.length === 0) {
    return [];
  }

  const batch = txHashes.map((txHash, index) => ({
    id: index + 1,
    jsonrpc: "2.0",
    method: "eth_getTransactionReceipt",
    params: [txHash],
  }));
  const payload = await postRpc<RpcBatchResponse<RpcReceipt | null> | RpcResponse<RpcReceipt | null>>(
    rpcUrl,
    batch,
  );

  if (Array.isArray(payload)) {
    const receiptById = new Map(payload.map((entry) => [entry.id, requireResult(entry)]));

    return txHashes
      .map((_, index) => receiptById.get(index + 1) ?? null)
      .filter((receipt): receipt is RpcReceipt => receipt !== null);
  }

  const receipts = await Promise.all(
    txHashes.map((txHash) =>
      fetchRpcMethod<RpcReceipt | null>(rpcUrl, "eth_getTransactionReceipt", [txHash]),
    ),
  );

  return receipts.filter((receipt): receipt is RpcReceipt => receipt !== null);
};

export const fetchTokenMetadata = async ({
  chain,
  address,
  rpcUrl,
}: {
  chain: Chain;
  address: string;
  rpcUrl: string;
}): Promise<TokenMetadata> => {
  const tokenAddress = normalizeAddress(address);

  const [nameHex, symbolHex, decimalsHex] = await Promise.all([
    callToken(rpcUrl, tokenAddress, nameSelector),
    callToken(rpcUrl, tokenAddress, symbolSelector),
    callToken(rpcUrl, tokenAddress, decimalsSelector),
  ]);

  return {
    chain,
    address: tokenAddress,
    name: decodeStringResult(nameHex),
    symbol: decodeStringResult(symbolHex),
    decimals: decodeIntegerResult(decimalsHex),
  };
};

export const fetchTokenMetadataBatch = async ({
  chain,
  addresses,
  rpcUrl,
}: {
  chain: Chain;
  addresses: string[];
  rpcUrl: string;
}) => {
  const normalizedAddresses = [...new Set(addresses.map(normalizeAddress))];

  if (normalizedAddresses.length === 0) {
    return [];
  }

  return (await chunkValues(normalizedAddresses, 3).reduce(
    async (allResultsPromise, addressChunk) => {
      const allResults = await allResultsPromise;
      const requests = addressChunk.flatMap((address, index) => {
        const baseId = index * 3;

        return [
          {
            id: baseId + 1,
            jsonrpc: "2.0" as const,
            method: "eth_call",
            params: [
              {
                to: address,
                data: nameSelector,
              },
              "latest",
            ],
          },
          {
            id: baseId + 2,
            jsonrpc: "2.0" as const,
            method: "eth_call",
            params: [
              {
                to: address,
                data: symbolSelector,
              },
              "latest",
            ],
          },
          {
            id: baseId + 3,
            jsonrpc: "2.0" as const,
            method: "eth_call",
            params: [
              {
                to: address,
                data: decimalsSelector,
              },
              "latest",
            ],
          },
        ] satisfies RpcRequest[];
      });

      const responses = await fetchRpcBatch<string>(rpcUrl, requests);
      const responseById = new Map(
        responses
          .filter((response): response is RpcResponse<string> => typeof response.id === "number")
          .map((response) => [response.id, response]),
      );

      return [
        ...allResults,
        ...addressChunk.map((address, index) => {
          const baseId = index * 3;

          try {
            const nameHex = requireBatchResult(responseById, baseId + 1);
            const symbolHex = requireBatchResult(responseById, baseId + 2);
            const decimalsHex = requireBatchResult(responseById, baseId + 3);

            return {
              address,
              token: {
                chain,
                address,
                name: decodeStringResult(nameHex),
                symbol: decodeStringResult(symbolHex),
                decimals: decodeIntegerResult(decimalsHex),
              } satisfies TokenMetadata,
              error: null,
            };
          } catch (error) {
            return {
              address,
              token: null,
              error: error instanceof Error ? error.message : String(error),
            };
          }
        }),
      ];
    },
    Promise.resolve(
      [] as Array<{
        address: string;
        token: TokenMetadata | null;
        error: string | null;
      }>,
    ),
  ));
};
