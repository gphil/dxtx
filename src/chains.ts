import type { Chain } from "./types.js";

type ChainConfig = {
  chain: Chain;
  tokenListFile: string;
  sqdUrl?: string;
  rpcUrl?: string;
  rpcEnv: string;
  sqdEnv: string;
};

const configs: ChainConfig[] = [
  {
    chain: "ethereum",
    tokenListFile: "eth-tokens.csv",
    sqdUrl: "https://v2.archive.subsquid.io/network/ethereum-mainnet",
    rpcUrl: "https://ethereum-rpc.publicnode.com",
    rpcEnv: "RPC_ETHEREUM_HTTP",
    sqdEnv: "SQD_ETHEREUM_URL",
  },
  {
    chain: "base",
    tokenListFile: "base-tokens.csv",
    sqdUrl: "https://v2.archive.subsquid.io/network/base-mainnet",
    rpcUrl: "https://mainnet.base.org",
    rpcEnv: "RPC_BASE_HTTP",
    sqdEnv: "SQD_BASE_URL",
  },
  {
    chain: "arbitrum",
    tokenListFile: "arbitrum-tokens.csv",
    sqdUrl: "https://v2.archive.subsquid.io/network/arbitrum-one",
    rpcUrl: "https://arb1.arbitrum.io/rpc",
    rpcEnv: "RPC_ARBITRUM_HTTP",
    sqdEnv: "SQD_ARBITRUM_URL",
  },
  {
    chain: "optimism",
    tokenListFile: "optimism-tokens.csv",
    sqdUrl: "https://v2.archive.subsquid.io/network/optimism-mainnet",
    rpcUrl: "https://optimism-rpc.publicnode.com",
    rpcEnv: "RPC_OPTIMISM_HTTP",
    sqdEnv: "SQD_OPTIMISM_URL",
  },
  {
    chain: "polygon",
    tokenListFile: "polygon-tokens.csv",
    sqdUrl: "https://v2.archive.subsquid.io/network/polygon-mainnet",
    rpcUrl: "https://polygon-rpc.com",
    rpcEnv: "RPC_POLYGON_HTTP",
    sqdEnv: "SQD_POLYGON_URL",
  },
  {
    chain: "avalanche",
    tokenListFile: "avalanche-tokens.csv",
    sqdUrl: "https://v2.archive.subsquid.io/network/avalanche-mainnet",
    rpcUrl: "https://api.avax.network/ext/bc/C/rpc",
    rpcEnv: "RPC_AVALANCHE_HTTP",
    sqdEnv: "SQD_AVALANCHE_URL",
  },
  {
    chain: "unichain",
    tokenListFile: "unichain-tokens.csv",
    sqdUrl: "https://v2.archive.subsquid.io/network/unichain-mainnet",
    rpcEnv: "RPC_UNICHAIN_HTTP",
    sqdEnv: "SQD_UNICHAIN_URL",
  },
  {
    chain: "bsc",
    tokenListFile: "bsc-tokens.csv",
    sqdUrl: "https://v2.archive.subsquid.io/network/binance-mainnet",
    rpcUrl: "https://bsc-dataseed.bnbchain.org",
    rpcEnv: "RPC_BSC_HTTP",
    sqdEnv: "SQD_BSC_URL",
  },
];

const configByChain = new Map(configs.map((config) => [config.chain, config]));

export const supportedChains = configs.map((config) => config.chain);

export const getChainConfig = (chain: Chain) => {
  const config = configByChain.get(chain);

  if (!config) {
    throw new Error(`unsupported chain: ${chain}`);
  }

  return config;
};

const requireValue = (value: string | undefined, message: string) => {
  if (!value) {
    throw new Error(message);
  }

  return value;
};

export const resolveRpcUrl = (
  chain: Chain,
  override?: string,
  env: NodeJS.ProcessEnv = process.env,
) => {
  if (override) {
    return override;
  }

  const config = getChainConfig(chain);
  return requireValue(
    env[config.rpcEnv] || config.rpcUrl,
    `missing RPC URL for ${chain}; set --rpc-url or ${config.rpcEnv}`,
  );
};

export const resolveSqdUrl = (
  chain: Chain,
  override?: string,
  env: NodeJS.ProcessEnv = process.env,
) => {
  if (override) {
    return override;
  }

  const config = getChainConfig(chain);
  return requireValue(
    env[config.sqdEnv] || config.sqdUrl,
    `missing SQD URL for ${chain}; set --sqd-url or ${config.sqdEnv}`,
  );
};
