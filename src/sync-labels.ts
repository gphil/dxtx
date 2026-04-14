import { access, readFile, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseDelimited } from "csv-parse/sync";
import { executeDuneSql } from "./dune.js";
import { logLine } from "./log.js";
import {
  buildCompositeDeleteSql,
  buildUpsertSql,
  chunkRows,
  cleanText,
  createServingClient,
  isDirectRun,
  normalizeAddress,
  normalizeNetwork,
  parseArgValue,
  parseBool,
  readTextFile,
} from "./serving.js";
import { ensureServingSchema } from "./serving-schema.js";

type RawAddressLabelRow = {
  network: string;
  address: string;
  source_name: string;
  source_key: string;
  source_type: string;
  source_uri: string | null;
  label: string | null;
  category: string | null;
  entity_type: string | null;
  aggregator_label: string | null;
  aggregator_source: string | null;
  upstream_aggregator_label: string | null;
  upstream_aggregator_source: string | null;
  confidence: number | null;
  metadata: Record<string, unknown>;
  source_recorded_at: Date | null;
  external_added_at: Date | null;
  inserted_at: Date;
  updated_at: Date;
};

type DuneSourceConfig = {
  sourceName: string;
  sourceType?: string;
  sourceUri?: string;
  mode?: "snapshot" | "incremental";
  defaultNetwork?: string;
  defaultConfidence?: number;
  sql: string;
  networkColumn?: string;
  addressColumn: string;
  labelColumn: string;
  categoryColumn?: string;
  entityTypeColumn?: string;
  aggregatorLabelColumn?: string;
  aggregatorSourceColumn?: string;
  upstreamAggregatorLabelColumn?: string;
  upstreamAggregatorSourceColumn?: string;
  externalAddedAtColumn?: string;
};

const currentDir = dirname(fileURLToPath(import.meta.url));
const vendoredPath = (fileName: string) => resolve(currentDir, "..", "vendor", "address-labels", fileName);
const defaultDuneConfigPath = resolve(currentDir, "..", "config", "dune-address-label-sources.json");

const supportedSupersetChains = new Set([
  "ethereum",
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
]);

const priorityCaseSql = `
  case
    when source_name = 'eigen_manual_labels' then 10
    when source_name = 'superset_cr_router_labels' then 20
    when source_name = 'dune_cex_labels' then 30
    when source_name like 'dune_%' then 40
    when source_name = 'eigen_cex_labels' then 50
    when source_name = 'eigen_etherscan_labels' then 60
    when source_name = 'eigen_names' then 70
    else 100
  end
`;

const resolveLabelsSql = `
  with active_networks as (
    select distinct network
    from token_flow_leaderboards
  ),
  candidates as (
    select
      case
        when raw.network = '*' then active_networks.network
        else raw.network
      end as network,
      raw.address,
      raw.label,
      raw.category,
      raw.entity_type,
      raw.aggregator_label,
      raw.aggregator_source,
      raw.upstream_aggregator_label,
      raw.upstream_aggregator_source,
      raw.source_name,
      raw.source_key,
      ${priorityCaseSql} as source_rank,
      raw.confidence,
      raw.metadata,
      raw.inserted_at,
      raw.updated_at,
      raw.source_recorded_at,
      raw.external_added_at
    from address_labels_raw as raw
    inner join active_networks
      on raw.network = '*'
      or raw.network = active_networks.network
    where raw.label is not null
      and raw.label <> ''
  ),
  ranked as (
    select
      candidates.*,
      row_number() over (
        partition by candidates.network, candidates.address
        order by
          candidates.source_rank asc,
          coalesce(candidates.confidence, 0) desc,
          coalesce(candidates.external_added_at, candidates.source_recorded_at, candidates.updated_at, candidates.inserted_at) desc,
          candidates.source_name asc,
          candidates.source_key asc
      ) as row_number
    from candidates
  )
  insert into address_labels (
    network,
    address,
    label,
    category,
    entity_type,
    aggregator_label,
    aggregator_source,
    upstream_aggregator_label,
    upstream_aggregator_source,
    source_name,
    source_key,
    source_rank,
    confidence,
    metadata,
    inserted_at,
    updated_at,
    source_recorded_at,
    external_added_at
  )
  select
    network,
    address,
    label,
    category,
    entity_type,
    aggregator_label,
    aggregator_source,
    upstream_aggregator_label,
    upstream_aggregator_source,
    source_name,
    source_key,
    source_rank,
    confidence,
    metadata,
    inserted_at,
    updated_at,
    source_recorded_at,
    external_added_at
  from ranked
  where row_number = 1
  on conflict (network, address) do update
  set
    label = excluded.label,
    category = excluded.category,
    entity_type = excluded.entity_type,
    aggregator_label = excluded.aggregator_label,
    aggregator_source = excluded.aggregator_source,
    upstream_aggregator_label = excluded.upstream_aggregator_label,
    upstream_aggregator_source = excluded.upstream_aggregator_source,
    source_name = excluded.source_name,
    source_key = excluded.source_key,
    source_rank = excluded.source_rank,
    confidence = excluded.confidence,
    metadata = excluded.metadata,
    updated_at = excluded.updated_at,
    source_recorded_at = excluded.source_recorded_at,
    external_added_at = excluded.external_added_at
`;

const parseRows = (text: string, delimiter: "," | "\t") =>
  parseDelimited(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    bom: true,
    delimiter,
  }) as Record<string, string>[];

const categoryFromNameTags = (tags: string | null) => {
  if (!tags) {
    return null;
  }

  if (tags.includes("CEX")) {
    return "cex";
  }

  if (tags.includes("Contract")) {
    return "contract";
  }

  if (tags.includes("ERC20") || tags.includes("ERC721")) {
    return "token";
  }

  return null;
};

const parseSourceRecordedAt = (path: string) => stat(path).then((result) => result.mtime);

const buildRawLabel = ({
  network,
  address,
  sourceName,
  sourceType,
  sourceUri,
  label,
  category,
  entityType,
  aggregatorLabel,
  aggregatorSource,
  upstreamAggregatorLabel,
  upstreamAggregatorSource,
  confidence,
  metadata,
  sourceRecordedAt,
  externalAddedAt,
}: {
  network: string;
  address: string;
  sourceName: string;
  sourceType: string;
  sourceUri: string | null;
  label: string | null;
  category: string | null;
  entityType: string | null;
  aggregatorLabel: string | null;
  aggregatorSource: string | null;
  upstreamAggregatorLabel: string | null;
  upstreamAggregatorSource: string | null;
  confidence: number | null;
  metadata: Record<string, unknown>;
  sourceRecordedAt: Date | null;
  externalAddedAt: Date | null;
}) => {
  const now = new Date();
  return {
    network,
    address,
    source_name: sourceName,
    source_key: `${network}:${address}`,
    source_type: sourceType,
    source_uri: sourceUri,
    label,
    category,
    entity_type: entityType,
    aggregator_label: aggregatorLabel,
    aggregator_source: aggregatorSource,
    upstream_aggregator_label: upstreamAggregatorLabel,
    upstream_aggregator_source: upstreamAggregatorSource,
    confidence,
    metadata,
    source_recorded_at: sourceRecordedAt,
    external_added_at: externalAddedAt,
    inserted_at: now,
    updated_at: now,
  } satisfies RawAddressLabelRow;
};

const loadEigenManualLabels = async () => {
  const sourcePath = vendoredPath("manual_labels.csv");
  const sourceRecordedAt = await parseSourceRecordedAt(sourcePath);
  const rows = parseRows(await readTextFile(sourcePath), ",");

  return rows.flatMap((row) => {
    const address = normalizeAddress(row.address);
    const label = cleanText(row.manual_label);

    return address && label
      ? [
          buildRawLabel({
            network: "eth",
            address,
            sourceName: "eigen_manual_labels",
            sourceType: "file",
            sourceUri: sourcePath,
            label,
            category: null,
            entityType: null,
            aggregatorLabel: null,
            aggregatorSource: null,
            upstreamAggregatorLabel: null,
            upstreamAggregatorSource: null,
            confidence: 1,
            metadata: row,
            sourceRecordedAt,
            externalAddedAt: null,
          }),
        ]
      : [];
  });
};

const loadEigenCexLabels = async () => {
  const sourcePath = vendoredPath("cex_labels.csv");
  const sourceRecordedAt = await parseSourceRecordedAt(sourcePath);
  const rows = parseRows(await readTextFile(sourcePath), ",");

  return rows.flatMap((row) => {
    const address = normalizeAddress(row.address);
    const label = cleanText(row.cex_name);

    return address && label
      ? [
          buildRawLabel({
            network: "eth",
            address,
            sourceName: "eigen_cex_labels",
            sourceType: "file",
            sourceUri: sourcePath,
            label,
            category: "cex",
            entityType: "exchange",
            aggregatorLabel: null,
            aggregatorSource: null,
            upstreamAggregatorLabel: null,
            upstreamAggregatorSource: null,
            confidence: 0.85,
            metadata: row,
            sourceRecordedAt,
            externalAddedAt: null,
          }),
        ]
      : [];
  });
};

const loadEigenEtherscanLabels = async () => {
  const sourcePath = vendoredPath("EIGEN_etherscan_labels_031625.csv");
  const sourceRecordedAt = await parseSourceRecordedAt(sourcePath);
  const rows = parseRows(await readTextFile(sourcePath), ",");

  return rows.flatMap((row) => {
    const address = normalizeAddress(row.address);
    const contractName = cleanText(row.contract_name);
    const walletLabel = cleanText(row.wallet_label);
    const label = walletLabel || contractName;
    const category = parseBool(row.exchange_flag) ? "cex" : cleanText(row.contract_type) || cleanText(row.wallet_type);
    const entityType = parseBool(row.is_contract) ? "contract" : null;

    return address && label
      ? [
          buildRawLabel({
            network: "eth",
            address,
            sourceName: "eigen_etherscan_labels",
            sourceType: "file",
            sourceUri: sourcePath,
            label,
            category,
            entityType,
            aggregatorLabel: null,
            aggregatorSource: null,
            upstreamAggregatorLabel: null,
            upstreamAggregatorSource: null,
            confidence: 0.75,
            metadata: row,
            sourceRecordedAt,
            externalAddedAt: null,
          }),
        ]
      : [];
  });
};

const loadEigenNames = async () => {
  const sourcePath = vendoredPath("names.tab");
  const sourceRecordedAt = await parseSourceRecordedAt(sourcePath);
  const rows = parseRows(await readTextFile(sourcePath), "\t");

  return rows.flatMap((row) => {
    const address = normalizeAddress(row.address);
    const label = cleanText(row.name);

    return address && label
      ? [
          buildRawLabel({
            network: "eth",
            address,
            sourceName: "eigen_names",
            sourceType: "file",
            sourceUri: sourcePath,
            label,
            category: categoryFromNameTags(cleanText(row.tags)),
            entityType: parseBool(row.isContract) ? "contract" : null,
            aggregatorLabel: null,
            aggregatorSource: null,
            upstreamAggregatorLabel: null,
            upstreamAggregatorSource: null,
            confidence: 0.6,
            metadata: row,
            sourceRecordedAt,
            externalAddedAt: null,
          }),
        ]
      : [];
  });
};

const splitSqlStatements = (sql: string) => {
  const source = sql.replace(/^--.*$/gm, "");
  const state = {
    statements: [] as string[],
    current: "",
    inQuote: false,
  };

  const finalState = [...source].reduce((result, character, index, allCharacters) => {
    const nextCharacter = allCharacters[index + 1];

    if (character === "'" && result.inQuote && nextCharacter === "'") {
      result.current += "''";
      allCharacters[index + 1] = "";
      return result;
    }

    if (character === "'") {
      result.inQuote = !result.inQuote;
    }

    if (character === ";" && !result.inQuote) {
      const statement = result.current.trim();

      if (statement.length > 0) {
        result.statements.push(statement);
      }

      result.current = "";
      return result;
    }

    result.current += character;
    return result;
  }, state);

  const tail = finalState.current.trim();
  return tail.length > 0 ? [...finalState.statements, tail] : finalState.statements;
};

const loadSupersetRouterLabels = async () => {
  const sourcePath = vendoredPath("superset_cr_router_labels.sql");
  const sourceRecordedAt = await parseSourceRecordedAt(sourcePath);
  const sql = await readTextFile(sourcePath);
  const statements = splitSqlStatements(sql).filter((statement) => {
    const normalized = statement.trim().toLowerCase();
    return (
      normalized.startsWith("create table if not exists analysis.router_labels") ||
      normalized.startsWith("alter table analysis.router_labels") ||
      normalized.startsWith("insert into analysis.router_labels") ||
      normalized.startsWith("update analysis.router_labels") ||
      normalized.startsWith("create index if not exists idx_router_labels_address")
    );
  });
  const client = createServingClient();
  await client.connect();

  try {
    await client.query("begin");
    await client.query("create schema if not exists analysis");
    await client.query("drop table if exists analysis.router_labels");

    for (const statement of statements) {
      await client.query(statement);
    }

    const result = await client.query<{
      chain: string;
      address: string;
      label: string;
      category: string | null;
      entity_type: string | null;
      aggregator_label: string | null;
      aggregator_source: string | null;
      upstream_aggregator_label: string | null;
      upstream_aggregator_source: string | null;
      source: string | null;
      notes: string | null;
    }>(`
      select
        chain,
        address,
        label,
        category,
        entity_type,
        aggregator_label,
        aggregator_source,
        upstream_aggregator_label,
        upstream_aggregator_source,
        source,
        notes
      from analysis.router_labels
      order by chain, address
    `);
    await client.query("rollback");

    return result.rows.flatMap((row) => {
      if (!supportedSupersetChains.has(row.chain)) {
        return [];
      }

      const network = normalizeNetwork(row.chain);
      const address = normalizeAddress(row.address);

      return network && address
        ? [
            buildRawLabel({
              network,
              address,
              sourceName: "superset_cr_router_labels",
              sourceType: "sql",
              sourceUri: sourcePath,
              label: cleanText(row.label),
              category: cleanText(row.category),
              entityType: cleanText(row.entity_type),
              aggregatorLabel: cleanText(row.aggregator_label),
              aggregatorSource: cleanText(row.aggregator_source),
              upstreamAggregatorLabel: cleanText(row.upstream_aggregator_label),
              upstreamAggregatorSource: cleanText(row.upstream_aggregator_source),
              confidence: 0.95,
              metadata: { source: row.source, notes: row.notes, chain: row.chain },
              sourceRecordedAt,
              externalAddedAt: null,
            }),
          ]
        : [];
    });
  } catch (error) {
    await client.query("rollback").catch(() => undefined);
    throw error;
  } finally {
    await client.end();
  }
};

const resolvedDuneConfigPath = async () => {
  const configured = parseArgValue("dune-config") || process.env.ADDRESS_LABELS_DUNE_CONFIG || null;

  if (configured) {
    return resolve(process.cwd(), configured);
  }

  try {
    await access(defaultDuneConfigPath);
    return defaultDuneConfigPath;
  } catch {
    return null;
  }
};

const loadDuneSources = async (configPath: string | null) => {
  if (!configPath) {
    return [] as RawAddressLabelRow[];
  }

  const configs = JSON.parse(await readFile(configPath, "utf8")) as DuneSourceConfig[];
  const client = createServingClient();
  await client.connect();

  try {
    const loadedGroups = await Promise.all(
      configs.map(async (config) => {
        const prior = await client.query<{ external_added_at: string | null }>(
          `
            select max(external_added_at)::text as external_added_at
            from address_labels_raw
            where source_name = $1
          `,
          [config.sourceName],
        );
        const externalAddedAfter = prior.rows[0]?.external_added_at ?? null;
        const externalAddedAfterSql =
          externalAddedAfter === null ? "null" : `'${externalAddedAfter.replaceAll("'", "''")}'`;
        const sql = config.sql.replaceAll("{{external_added_after}}", externalAddedAfterSql);
        const rows = await executeDuneSql<Record<string, unknown>>({ sql });
        const sourceRecordedAt = new Date();

        return rows.flatMap((row) => {
          const network =
            normalizeNetwork(cleanText(config.networkColumn ? row[config.networkColumn] : config.defaultNetwork)) ||
            normalizeNetwork(config.defaultNetwork || "*");
          const address = normalizeAddress(cleanText(row[config.addressColumn]));
          const label = cleanText(row[config.labelColumn]);

          return network && address && label
            ? [
                buildRawLabel({
                  network,
                  address,
                  sourceName: config.sourceName,
                  sourceType: config.sourceType || "dune",
                  sourceUri: config.sourceUri || null,
                  label,
                  category: cleanText(config.categoryColumn ? row[config.categoryColumn] : null),
                  entityType: cleanText(config.entityTypeColumn ? row[config.entityTypeColumn] : null),
                  aggregatorLabel: cleanText(config.aggregatorLabelColumn ? row[config.aggregatorLabelColumn] : null),
                  aggregatorSource: cleanText(
                    config.aggregatorSourceColumn ? row[config.aggregatorSourceColumn] : null,
                  ),
                  upstreamAggregatorLabel: cleanText(
                    config.upstreamAggregatorLabelColumn ? row[config.upstreamAggregatorLabelColumn] : null,
                  ),
                  upstreamAggregatorSource: cleanText(
                    config.upstreamAggregatorSourceColumn ? row[config.upstreamAggregatorSourceColumn] : null,
                  ),
                  confidence: config.defaultConfidence || 0.8,
                  metadata: row,
                  sourceRecordedAt,
                  externalAddedAt:
                    config.externalAddedAtColumn && cleanText(row[config.externalAddedAtColumn])
                      ? new Date(String(row[config.externalAddedAtColumn]))
                      : null,
                }),
              ]
            : [];
        });
      }),
    );

    return loadedGroups.flat();
  } finally {
    await client.end();
  }
};

const deleteMissingSnapshotRows = async ({
  client,
  sourceName,
  currentRows,
}: {
  client: ReturnType<typeof createServingClient>;
  sourceName: string;
  currentRows: RawAddressLabelRow[];
}) => {
  const existing = await client.query<{ network: string; address: string; source_key: string }>(
    `
      select network, address, source_key
      from address_labels_raw
      where source_name = $1
    `,
    [sourceName],
  );
  const currentKeys = new Set(currentRows.map((row) => `${row.network}:${row.address}:${row.source_key}`));
  const rowsToDelete = existing.rows.filter(
    (row) => !currentKeys.has(`${row.network}:${row.address}:${row.source_key}`),
  );

  for (const chunk of chunkRows(rowsToDelete, 1000)) {
    const { text, params } = buildCompositeDeleteSql({
      table: "address_labels_raw",
      keyColumns: ["network", "address", "source_key"],
      rows: chunk as unknown as Record<string, unknown>[],
      extraWhereColumn: "source_name",
      extraWhereValue: sourceName,
    });
    await client.query(text, params);
  }

  return rowsToDelete.length;
};

const upsertRawRows = async (client: ReturnType<typeof createServingClient>, rows: RawAddressLabelRow[]) => {
  const columns = [
    "network",
    "address",
    "source_name",
    "source_key",
    "source_type",
    "source_uri",
    "label",
    "category",
    "entity_type",
    "aggregator_label",
    "aggregator_source",
    "upstream_aggregator_label",
    "upstream_aggregator_source",
    "confidence",
    "metadata",
    "source_recorded_at",
    "external_added_at",
    "inserted_at",
    "updated_at",
  ];

  for (const chunk of chunkRows(rows, 1000)) {
    const { text, params } = buildUpsertSql({
      table: "address_labels_raw",
      columns,
      rows: chunk as unknown as Record<string, unknown>[],
      conflict: ["network", "address", "source_name", "source_key"],
      updates: columns.filter((column) => column !== "inserted_at").slice(4),
    });
    await client.query(text, params);
  }
};

export const syncLabels = async () => {
  await ensureServingSchema();
  const skipLocal = parseBool(parseArgValue("skip-local"));
  const duneConfigPath = await resolvedDuneConfigPath();
  const startedAt = performance.now();
  const client = createServingClient();
  const sourceGroups = skipLocal
    ? []
    : [
        { sourceName: "eigen_manual_labels", rows: await loadEigenManualLabels() },
        { sourceName: "eigen_cex_labels", rows: await loadEigenCexLabels() },
        { sourceName: "eigen_etherscan_labels", rows: await loadEigenEtherscanLabels() },
        { sourceName: "eigen_names", rows: await loadEigenNames() },
        { sourceName: "superset_cr_router_labels", rows: await loadSupersetRouterLabels() },
      ];
  const duneRows = await loadDuneSources(duneConfigPath);
  const duneGroups = duneRows.reduce<Record<string, RawAddressLabelRow[]>>(
    (result, row) => ({ ...result, [row.source_name]: [...(result[row.source_name] || []), row] }),
    {},
  );

  await client.connect();

  try {
    await client.query("begin");

    for (const sourceGroup of sourceGroups) {
      const deletedRows = await deleteMissingSnapshotRows({
        client,
        sourceName: sourceGroup.sourceName,
        currentRows: sourceGroup.rows,
      });
      await upsertRawRows(client, sourceGroup.rows);
      logLine("synced address label snapshot", {
        source_name: sourceGroup.sourceName,
        rows: sourceGroup.rows.length,
        deleted_rows: deletedRows,
      });
    }

    for (const [sourceName, rows] of Object.entries(duneGroups)) {
      await upsertRawRows(client, rows);
      logLine("synced address label dune source", { source_name: sourceName, rows: rows.length });
    }

    await client.query("truncate table address_labels");
    await client.query(resolveLabelsSql);
    await client.query("commit");

    const result = await client.query<{ count: string }>("select count(*)::text as count from address_labels");
    logLine("completed address label sync", {
      resolved_rows: Number(result.rows[0]?.count || 0),
      duration_ms: Math.round(performance.now() - startedAt),
    });
  } catch (error) {
    await client.query("rollback").catch(() => undefined);
    throw error;
  } finally {
    await client.end();
  }
};

if (isDirectRun(import.meta.url)) {
  syncLabels().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
