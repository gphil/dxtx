import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "pg";

export const supportedNetworks = [
  "eth",
  "base",
  "polygon",
  "arbitrum",
  "bsc",
  "optimism",
  "avalanche",
  "zora",
  "unichain",
  "blast",
  "ink",
] as const;

export type SupportedNetwork = (typeof supportedNetworks)[number];

const networkMap: Record<string, SupportedNetwork> = {
  ethereum: "eth",
  eth: "eth",
  base: "base",
  polygon: "polygon",
  polygon_pos: "polygon",
  arbitrum: "arbitrum",
  bsc: "bsc",
  binance: "bsc",
  optimism: "optimism",
  avalanche: "avalanche",
  zora: "zora",
  unichain: "unichain",
  blast: "blast",
  ink: "ink",
};

const hasValue = (value: string | undefined) => value !== undefined && value !== "";

export const servingDatabaseUrl = () => {
  const directUrl = process.env.FLOWS_DATABASE_URL;

  if (hasValue(directUrl)) {
    return directUrl;
  }

  const dbUser = process.env.DB_USER;
  const dbPass = process.env.DB_PASS;
  const dbName = process.env.DB_NAME;
  const dbPort = process.env.DB_PORT;
  const dbHost = process.env.DB_HOST;

  if (![dbUser, dbPass, dbName, dbPort, dbHost].some(hasValue)) {
    return undefined;
  }

  const user = encodeURIComponent(dbUser || "squid");
  const password = encodeURIComponent(dbPass || "squid");
  const host = dbHost || "127.0.0.1";
  const port = dbPort || "15432";
  const database = encodeURIComponent(dbName || "dxtx_flows");

  return `postgresql://${user}:${password}@${host}:${port}/${database}`;
};

export const createServingClient = () => {
  const connectionString = servingDatabaseUrl();

  if (!connectionString) {
    throw new Error("missing postgres serving database configuration");
  }

  return new Client({ connectionString });
};

export const normalizeNetwork = (value: string | null | undefined): SupportedNetwork | "*" | null => {
  if (!value) {
    return null;
  }

  if (value === "*") {
    return "*";
  }

  return networkMap[value.trim().toLowerCase()] ?? null;
};

export const normalizeAddress = (value: string | null | undefined) => {
  if (!value) {
    return null;
  }

  const address = value.trim().toLowerCase();
  return /^0x[a-f0-9]{40}$/.test(address) ? address : null;
};

export const cleanText = (value: unknown) => {
  if (value === null || value === undefined) {
    return null;
  }

  const text = String(value).trim().replace(/^"(.*)"$/, "$1").trim();
  return text.length > 0 ? text : null;
};

export const parseBool = (value: unknown) => {
  const text = cleanText(value)?.toLowerCase();
  return text === "true" || text === "t" || text === "1" || text === "yes";
};

export const dayText = (value: Date | string) =>
  (typeof value === "string" ? new Date(`${value}T00:00:00Z`) : value).toISOString().slice(0, 10);

export const nowDayText = () => new Date().toISOString().slice(0, 10);

export const nextDayText = (value: string) =>
  new Date(Date.parse(`${value}T00:00:00Z`) + 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

export const addDays = (value: string, days: number) =>
  new Date(Date.parse(`${value}T00:00:00Z`) + days * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

export const chunkRows = <T>(values: T[], size: number) =>
  Array.from({ length: Math.ceil(values.length / size) }, (_, index) =>
    values.slice(index * size, (index + 1) * size),
  );

export const median = (values: number[]) => {
  if (values.length === 0) {
    return null;
  }

  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  const middleValue = sorted[middle];

  if (sorted.length % 2 === 1) {
    return middleValue ?? null;
  }

  const leftValue = sorted[middle - 1];
  return leftValue === undefined || middleValue === undefined ? null : (leftValue + middleValue) / 2;
};

const normalizePgValue = (value: unknown): unknown =>
  typeof value === "bigint" ? value.toString() : value;

export const buildUpsertSql = ({
  table,
  columns,
  rows,
  conflict,
  updates,
}: {
  table: string;
  columns: string[];
  rows: Record<string, unknown>[];
  conflict: string[];
  updates: string[];
}) => {
  const values = rows
    .map(
      (_, rowIndex) =>
        `(${columns.map((__, columnIndex) => `$${rowIndex * columns.length + columnIndex + 1}`).join(", ")})`,
    )
    .join(", ");
  const params = rows.flatMap((row) => columns.map((column) => normalizePgValue(row[column])));

  return {
    text: `
      insert into ${table} (${columns.join(", ")})
      values ${values}
      on conflict (${conflict.join(", ")}) do update
      set ${updates.map((column) => `${column} = excluded.${column}`).join(", ")}
    `,
    params,
  };
};

export const buildCompositeDeleteSql = ({
  table,
  keyColumns,
  rows,
  extraWhereColumn,
  extraWhereValue,
}: {
  table: string;
  keyColumns: string[];
  rows: Record<string, unknown>[];
  extraWhereColumn?: string;
  extraWhereValue?: unknown;
}) => {
  const baseOffset = extraWhereColumn ? 1 : 0;
  const valueTuples = rows
    .map((_, rowIndex) => {
      const rowOffset = baseOffset + rowIndex * keyColumns.length;
      return `(${keyColumns.map((__, columnIndex) => `$${rowOffset + columnIndex + 1}`).join(", ")})`;
    })
    .join(", ");
  const whereClauses = [
    extraWhereColumn ? `${extraWhereColumn} = $1` : null,
    `(${keyColumns.join(", ")}) in (${valueTuples})`,
  ].filter(Boolean);
  const params = [
    ...(extraWhereColumn ? [extraWhereValue] : []),
    ...rows.flatMap((row) => keyColumns.map((column) => normalizePgValue(row[column]))),
  ];

  return {
    text: `delete from ${table} where ${whereClauses.join(" and ")}`,
    params,
  };
};

export const parseArgValue = (name: string) =>
  process.argv.slice(2).find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3) ?? null;

export const parseIntegerArg = (name: string) => {
  const value = parseArgValue(name);
  return value === null ? null : Number.parseInt(value, 10);
};

export const isDirectRun = (metaUrl: string) => {
  const entry = process.argv[1];
  return entry ? resolve(entry) === fileURLToPath(metaUrl) : false;
};

export const readTextFile = (path: string) => readFile(path, "utf8");
