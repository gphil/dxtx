export type Chain =
  | "ethereum"
  | "base"
  | "arbitrum"
  | "optimism"
  | "polygon"
  | "avalanche"
  | "unichain"
  | "bsc";

export type TokenMetadata = {
  chain: Chain;
  address: string;
  name: string;
  symbol: string;
  decimals: number;
};

export type TransferChunk = {
  cacheUri: string;
  tokenAddress?: string;
  fromBlock: number;
  toBlock: number;
};

export type TokenTarget = {
  chain: Chain;
  address: string;
  source: string;
  coingeckoId?: string;
  coingeckoName?: string;
  coingeckoSymbol?: string;
};
