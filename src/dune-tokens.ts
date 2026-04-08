import { executeDuneSql } from "./dune.js";
import { normalizeAddress } from "./format.js";
import type { Chain, TokenMetadata } from "./types.js";

const duneChainPairs = [
  ["ethereum", "ethereum"],
  ["base", "base"],
  ["arbitrum", "arbitrum"],
  ["optimism", "optimism"],
  ["polygon", "polygon"],
  ["avalanche", "avalanche_c"],
  ["unichain", "unichain"],
  ["bsc", "bnb"],
] as const satisfies ReadonlyArray<readonly [Chain, string]>;

const duneBlockchainByChain = new Map(duneChainPairs);
const chainByDuneBlockchain = new Map(
  duneChainPairs.map(([chain, duneBlockchain]) => [duneBlockchain, chain] as const),
);

type DuneTokenRow = {
  blockchain: (typeof duneChainPairs)[number][1];
  address: string;
  symbol: string | null;
  name: string | null;
  decimals: number | string | null;
};

const quote = (value: string) => `'${value.replaceAll("'", "''")}'`;
const chunkValues = <T>(values: T[], chunkSize: number) =>
  Array.from({ length: Math.ceil(values.length / chunkSize) }, (_, index) =>
    values.slice(index * chunkSize, (index + 1) * chunkSize),
  );

const toTokenMetadata = (row: DuneTokenRow): TokenMetadata | null => {
  const chain = chainByDuneBlockchain.get(row.blockchain);

  if (!chain || !row.name || !row.symbol || row.decimals === null) {
    return null;
  }

  return {
    chain,
    address: normalizeAddress(row.address),
    name: row.name,
    symbol: row.symbol,
    decimals: Number(row.decimals),
  } satisfies TokenMetadata;
};

export const loadDuneTokenMetadata = async ({
  chain,
  addresses,
  apiKey = process.env.DUNE_API_KEY,
  chunkSize = 500,
}: {
  chain: Chain;
  addresses: string[];
  apiKey?: string;
  chunkSize?: number;
}) => {
  const duneBlockchain = duneBlockchainByChain.get(chain);
  const normalizedAddresses = [...new Set(addresses.map(normalizeAddress))];

  if (!apiKey || !duneBlockchain || normalizedAddresses.length === 0) {
    return [];
  }

  return (await chunkValues(normalizedAddresses, chunkSize).reduce(
    async (allTokensPromise, addressChunk) => {
      const allTokens = await allTokensPromise;
      const rows = await executeDuneSql<DuneTokenRow>({
        apiKey,
        sql: `
          select
            blockchain,
            lower(cast(contract_address as varchar)) as address,
            symbol,
            name,
            decimals
          from tokens.erc20
          where blockchain = ${quote(duneBlockchain)}
            and lower(cast(contract_address as varchar)) in (${addressChunk.map(quote).join(", ")})
          order by address asc
        `,
      });

      return [
        ...allTokens,
        ...rows.flatMap((row) => {
          const token = toTokenMetadata(row);
          return token ? [token] : [];
        }),
      ];
    },
    Promise.resolve([] as TokenMetadata[]),
  ));
};

export const loadDuneUniverseTokenMetadata = async ({
  scopes,
  apiKey = process.env.DUNE_API_KEY,
}: {
  scopes: Array<{
    chain: Chain;
    addresses: string[];
  }>;
  apiKey?: string;
}) => {
  if (!apiKey) {
    return [];
  }

  const scopedRows = scopes.flatMap(({ chain, addresses }) => {
    const duneBlockchain = duneBlockchainByChain.get(chain);

    if (!duneBlockchain) {
      return [];
    }

    return [...new Set(addresses.map(normalizeAddress))].map(
      (address) => [duneBlockchain, address] as const,
    );
  });

  if (scopedRows.length === 0) {
    return [];
  }

  const rows = await executeDuneSql<DuneTokenRow>({
    apiKey,
    sql: `
      with scoped_tokens(blockchain, address) as (
        values
          ${scopedRows.map(([blockchain, address]) => `(${quote(blockchain)}, ${quote(address)})`).join(",\n          ")}
      )
      select
        scoped_tokens.blockchain,
        scoped_tokens.address,
        token.symbol,
        token.name,
        token.decimals
      from scoped_tokens
      join tokens.erc20 as token
        on token.blockchain = scoped_tokens.blockchain
       and lower(cast(token.contract_address as varchar)) = scoped_tokens.address
      where token.name is not null
        and token.symbol is not null
        and token.decimals is not null
      order by scoped_tokens.blockchain, scoped_tokens.address
    `,
  });

  return rows.flatMap((row) => {
    const token = toTokenMetadata(row);
    return token ? [token] : [];
  });
};
