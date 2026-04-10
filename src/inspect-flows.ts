import { DuckDBInstance, type DuckDBConnection } from "@duckdb/node-api";
import { escapeSqlString } from "./format.js";
import { supportedChains } from "./chains.js";
import type { Chain } from "./types.js";

const splitArgs = (values: string[]) =>
  values.flatMap((value) => value.split(",")).map((value) => value.trim()).filter(Boolean);

const isChain = (value: string): value is Chain => supportedChains.includes(value as Chain);

const parseArgs = (args: string[]) => {
  const [first, ...rest] = splitArgs(args);

  if (!first || !isChain(first)) {
    throw new Error(`usage: npm run inspect:flows -- <chain> <token-symbol|coingecko-id|address>`);
  }

  const token = rest.join(" ").trim();

  if (!token) {
    throw new Error(`usage: npm run inspect:flows -- <chain> <token-symbol|coingecko-id|address>`);
  }

  return { chain: first, token };
};

const outputPath = (chain: Chain) =>
  process.env.ANALYTICS_DB_PATH || `./analytics/${chain}-flows.duckdb`;

const createConnection = async (databasePath: string) => {
  const instance = await DuckDBInstance.create(databasePath);
  return instance.connect();
};

const rows = async <T>(connection: DuckDBConnection, sql: string) =>
  (await connection.runAndReadAll(sql)).getRowObjectsJS() as T[];

type TokenRow = {
  chain: string;
  address: string;
  token_name: string;
  token_symbol: string;
  coingecko_id: string | null;
};

type FlowSummary = {
  days: number;
  first_day: string | null;
  last_day: string | null;
  total_transfers: number;
  total_amount_native: number;
};

type DailyTotalRow = {
  day: string;
  transfer_count: number;
  amount_native_sum: number;
  avg_amount_native: number;
};

type FlowRow = {
  flow_rank: number;
  address: string;
  transfer_count: number;
  amount_native_sum: number;
  avg_amount_native: number;
};

type NetFlowRow = {
  address: string;
  sent_transfer_count: number;
  received_transfer_count: number;
  net_amount_native_sum: number;
  gross_amount_native_sum: number;
};

const tokenLookupSql = (token: string) => {
  const value = escapeSqlString(token.toLowerCase());
  const addressMatch = token.startsWith("0x")
    ? `lower(address) = '${value}'`
    : "false";

  return `
    select
      chain,
      address,
      token_name,
      token_symbol,
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

const summarySql = (address: string) => `
  select
    count(distinct day) as days,
    cast(min(day) as varchar) as first_day,
    cast(max(day) as varchar) as last_day,
    sum(transfer_count) as total_transfers,
    sum(amount_native_sum) as total_amount_native
  from token_daily_totals
  where token_address = '${escapeSqlString(address)}'
`;

const recentTotalsSql = (address: string) => `
  select
    cast(day as varchar) as day,
    transfer_count,
    amount_native_sum,
    avg_amount_native
  from token_daily_totals
  where token_address = '${escapeSqlString(address)}'
  order by day desc
  limit 10
`;

const topFlowsSql = ({
  address,
  direction,
  day,
}: {
  address: string;
  direction: "sender" | "recipient";
  day: string;
}) => `
  select
    flow_rank,
    address,
    transfer_count,
    amount_native_sum,
    avg_amount_native
  from token_daily_top_flows
  where token_address = '${escapeSqlString(address)}'
    and direction = '${direction}'
    and day = date '${escapeSqlString(day)}'
  order by flow_rank asc
  limit 10
`;

const topNetFlowsSql = ({
  address,
  direction,
  day,
}: {
  address: string;
  direction: "in" | "out";
  day: string;
}) => `
  select
    address,
    sent_transfer_count,
    received_transfer_count,
    net_amount_native_sum,
    gross_amount_native_sum
  from token_daily_address_flows
  where token_address = '${escapeSqlString(address)}'
    and day = date '${escapeSqlString(day)}'
    and ${direction === "in" ? "net_amount_native_sum > 0" : "net_amount_native_sum < 0"}
  order by
    ${direction === "in" ? "net_amount_native_sum desc" : "net_amount_native_sum asc"},
    gross_amount_native_sum desc,
    address asc
  limit 10
`;

const inspectToken = async ({
  connection,
  token,
}: {
  connection: DuckDBConnection;
  token: string;
}) => {
  const matches = await rows<TokenRow>(connection, tokenLookupSql(token));

  if (matches.length === 0) {
    throw new Error(`no token found for '${token}'`);
  }

  if (matches.length > 1) {
    console.log("multiple token matches:");
    console.table(matches);
    throw new Error(`token '${token}' is ambiguous; rerun with address`);
  }

  const [resolved] = matches;

  if (!resolved) {
    throw new Error(`no token found for '${token}'`);
  }

  const [summary] = await rows<FlowSummary>(connection, summarySql(resolved.address));

  if (!summary || summary.days === 0 || !summary.last_day) {
    console.log("token:");
    console.table([resolved]);
    throw new Error(`no flow rows found for ${resolved.address}`);
  }

  const recentTotals = await rows<DailyTotalRow>(connection, recentTotalsSql(resolved.address));
  const topSenders = await rows<FlowRow>(
    connection,
    topFlowsSql({ address: resolved.address, direction: "sender", day: summary.last_day }),
  );
  const topRecipients = await rows<FlowRow>(
    connection,
    topFlowsSql({ address: resolved.address, direction: "recipient", day: summary.last_day }),
  );
  const topNetInflows = await rows<NetFlowRow>(
    connection,
    topNetFlowsSql({ address: resolved.address, direction: "in", day: summary.last_day }),
  );
  const topNetOutflows = await rows<NetFlowRow>(
    connection,
    topNetFlowsSql({ address: resolved.address, direction: "out", day: summary.last_day }),
  );

  console.log("token:");
  console.table([resolved]);
  console.log("summary:");
  console.table([summary]);
  console.log(`recent daily totals (${resolved.token_symbol}):`);
  console.table(recentTotals);
  console.log(`top senders on ${summary.last_day}:`);
  console.table(topSenders);
  console.log(`top recipients on ${summary.last_day}:`);
  console.table(topRecipients);
  console.log(`top net inflows on ${summary.last_day}:`);
  console.table(topNetInflows);
  console.log(`top net outflows on ${summary.last_day}:`);
  console.table(topNetOutflows);
};

const main = async () => {
  const { chain, token } = parseArgs(process.argv.slice(2));
  const connection = await createConnection(outputPath(chain));

  try {
    await inspectToken({ connection, token });
  } finally {
    connection.closeSync();
  }
};

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exitCode = 1;
});
