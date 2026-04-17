import { Client } from "pg";
import { logLine } from "./log.js";

type QueryRow = Record<string, unknown>;

const hasValue = (value: string | undefined) => value !== undefined && value !== "";

const parseArgValue = (name: string) => {
  const flag = `--${name}`;
  const prefix = `${flag}=`;
  const arg = process.argv.slice(2).find((value) => value === flag || value.startsWith(prefix));

  return arg === undefined ? null : arg === flag ? "true" : arg.slice(prefix.length);
};

const parsePositiveInt = (value: string | null, fallback: number) => {
  const parsed = Number.parseInt(value || "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const databaseUrl = () => {
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
    return "postgresql://squid:squid@127.0.0.1:15432/dxtx_flows";
  }

  const user = encodeURIComponent(dbUser || "squid");
  const password = encodeURIComponent(dbPass || "squid");
  const host = dbHost || "127.0.0.1";
  const port = dbPort || "15432";
  const database = encodeURIComponent(dbName || "dxtx_flows");

  return `postgresql://${user}:${password}@${host}:${port}/${database}`;
};

const databaseSizesSql = `
  select
    datname as database,
    pg_size_pretty(pg_database_size(datname)) as size,
    pg_database_size(datname)::text as bytes
  from pg_database
  order by pg_database_size(datname) desc
`;

const relationSizesSql = `
  with relation_sizes as (
    select
      n.nspname as schema_name,
      c.relname as relation_name,
      case c.relkind
        when 'r' then 'table'
        when 'm' then 'materialized_view'
        when 'p' then 'partitioned_table'
        else c.relkind::text
      end as relation_kind,
      pg_relation_size(c.oid) as heap_bytes,
      pg_indexes_size(c.oid) as index_bytes,
      case
        when c.reltoastrelid <> 0 then pg_total_relation_size(c.reltoastrelid)
        else 0
      end as toast_bytes,
      pg_total_relation_size(c.oid) as total_bytes,
      coalesce(s.n_live_tup, greatest(c.reltuples, 0)::bigint) as live_rows_estimate,
      coalesce(s.n_dead_tup, 0) as dead_rows_estimate,
      coalesce(s.seq_scan, 0) as seq_scan,
      coalesce(s.idx_scan, 0) as idx_scan,
      s.last_vacuum,
      s.last_autovacuum,
      s.last_analyze,
      s.last_autoanalyze
    from pg_class as c
    join pg_namespace as n
      on n.oid = c.relnamespace
    left join pg_stat_user_tables as s
      on s.relid = c.oid
    where c.relkind in ('r', 'm', 'p')
      and n.nspname not in ('pg_catalog', 'information_schema')
      and n.nspname !~ '^pg_toast'
  )
  select
    schema_name || '.' || relation_name as relation,
    relation_kind,
    pg_size_pretty(total_bytes) as total,
    pg_size_pretty(heap_bytes) as heap,
    pg_size_pretty(index_bytes) as indexes,
    pg_size_pretty(toast_bytes) as toast,
    live_rows_estimate::text as live_rows_estimate,
    dead_rows_estimate::text as dead_rows_estimate,
    coalesce(round(dead_rows_estimate::numeric * 100 / nullif(live_rows_estimate + dead_rows_estimate, 0), 2)::text, '') as dead_pct,
    coalesce(round(total_bytes::numeric / nullif(live_rows_estimate, 0), 2)::text, '') as bytes_per_live_row,
    last_autovacuum,
    last_autoanalyze
  from relation_sizes
  order by total_bytes desc
  limit $1
`;

const indexSizesSql = `
  select
    schemaname || '.' || relname as table_name,
    indexrelname as index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) as size,
    pg_relation_size(indexrelid)::text as bytes,
    idx_scan::text as idx_scan
  from pg_stat_user_indexes
  order by pg_relation_size(indexrelid) desc
  limit $1
`;

const walSizeSql = `
  select
    count(*)::text as wal_files,
    pg_size_pretty(coalesce(sum(size), 0)::bigint) as size,
    coalesce(sum(size), 0)::bigint::text as bytes
  from pg_ls_waldir()
`;

const exactCountTables = [
  "token_flow_daily_totals",
  "token_daily_address_flows",
  "token_flow_leaderboards",
  "token_price_daily",
  "address_labels_raw",
  "address_label_source_checks",
  "address_labels",
  "token_flow_leaderboards_enriched",
];

const exactNetworkCountsSql = (tables: string[]) =>
  tables
    .map(
      (table) => `
        select
          '${table}' as table_name,
          network,
          count(*)::text as rows
        from ${table}
        group by network
      `,
    )
    .join("\nunion all\n");

const visibleLength = (value: unknown) =>
  value instanceof Date ? value.toISOString().length : String(value ?? "").length;

const renderValue = (value: unknown) =>
  value instanceof Date ? value.toISOString() : String(value ?? "");

const renderTable = (title: string, rows: QueryRow[]) => {
  console.log(`\n${title}`);

  if (rows.length === 0) {
    console.log("(no rows)");
    return;
  }

  const columns = Object.keys(rows[0] ?? {});
  const widths = columns.map((column) =>
    Math.max(column.length, ...rows.map((row) => visibleLength(row[column]))),
  );
  const renderRow = (values: string[]) =>
    values.map((value, index) => value.padEnd(widths[index] ?? value.length)).join("  ");

  console.log(renderRow(columns));
  console.log(renderRow(columns.map((column) => "-".repeat(column.length))));
  rows.forEach((row) => console.log(renderRow(columns.map((column) => renderValue(row[column])))));
};

const queryRows = async <T extends QueryRow>(
  client: Client,
  sql: string,
  params: unknown[] = [],
) => (await client.query<T>(sql, params)).rows;

const tryQueryRows = async <T extends QueryRow>(
  client: Client,
  title: string,
  sql: string,
  params: unknown[] = [],
) => {
  try {
    return await queryRows<T>(client, sql, params);
  } catch (error) {
    logLine("skipped postgres size section", {
      section: title,
      error: error instanceof Error ? error.message : String(error),
    });
    return [];
  }
};

const main = async () => {
  const limit = parsePositiveInt(parseArgValue("limit"), 30);
  const exactCounts = parseArgValue("exact-counts") === "true";
  const client = new Client({ connectionString: databaseUrl() });
  const startedAt = Date.now();

  await client.connect();

  try {
    renderTable("database sizes", await queryRows(client, databaseSizesSql));
    renderTable("wal size", await tryQueryRows(client, "wal size", walSizeSql));
    renderTable("largest relations", await queryRows(client, relationSizesSql, [limit]));
    renderTable("largest indexes", await queryRows(client, indexSizesSql, [limit]));

    if (exactCounts) {
      logLine("running exact postgres network counts", { tables: exactCountTables.length });
      renderTable(
        "exact network counts",
        await queryRows(client, `${exactNetworkCountsSql(exactCountTables)}\norder by table_name, network`),
      );
    }
  } finally {
    await client.end();
  }

  logLine("checked postgres size", {
    limit,
    exact_counts: exactCounts ? 1 : undefined,
    duration_ms: Date.now() - startedAt,
  });
};

main().catch((error) => {
  logLine("postgres size check failed", {
    error: error instanceof Error ? error.message : String(error),
  });
  process.exitCode = 1;
});
