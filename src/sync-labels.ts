import { access, readFile, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { parse as parseDelimited } from "csv-parse/sync";
import { executeDuneSql } from "./dune.js";
import {
  clearUploadedTable,
  ensureUploadedTable,
  insertUploadedCsv,
  normalizeUploadedTableName,
} from "./dune-uploads.js";
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
const uploadedDuneTableName = "dxtx_unlabeled_flow_addresses";
const uploadedDuneTableDescription =
  "Distinct DXTX flow-address universe only. Contains network and address columns without flow amounts, ranks, or other proprietary metadata.";

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
    when source_name = 'first_party_exchange_labels' then 11
    when source_name = 'internet_manual_labels' then 12
    when source_name = 'superset_cr_router_labels' then 20
    when source_name = 'dune_cex_labels' then 30
    when source_name = 'dune_owner_addresses' then 35
    when source_name = 'dune_uploaded_owner_labels' then 37
    when source_name = 'dune_identifier_labels' then 45
    when source_name = 'dune_uploaded_identifier_labels' then 47
    when source_name like 'dune_%' then 50
    when source_name = 'eigen_cex_labels' then 50
    when source_name = 'eigen_etherscan_labels' then 60
    when source_name = 'eth_labels' then 65
    when source_name = 'eigen_names' then 70
    when source_name = 'dune_uploaded_ens_labels' then 80
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

const groupRowsBySourceName = (rows: RawAddressLabelRow[]) =>
  rows.reduce<Record<string, RawAddressLabelRow[]>>((result, row) => {
    const group = result[row.source_name] || [];
    group.push(row);
    result[row.source_name] = group;
    return result;
  }, {});

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
  sourceKey,
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
  sourceKey?: string;
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
    source_key: sourceKey || `${network}:${address}`,
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

const ethLabelsChainMap: Record<string, string> = {
  "1": "eth",
  "10": "optimism",
  "56": "bsc",
  "8453": "base",
  "42161": "arbitrum",
};

const excludedEthLabels = new Set([
  "airdrop-hunter",
  "avs-operator",
  "blocked",
  "buidlguidl-builders",
  "burn",
  "charity",
  "contract-deployer",
  "deprecated",
  "friend-tech-users",
  "genesis",
  "genesis-address",
  "maker-vault-owner",
  "mev-bot",
  "nonprofit",
  "old-contract",
  "parity-bug",
  "proposer-fee-recipient",
  "retropgf-recipient",
  "sybil-delegate",
  "take-action",
  "token-sale",
]);

const ethLabelsCategory = (label: string, nameTag: string | null) => {
  const text = `${label} ${nameTag || ""}`.toLowerCase();

  if (
    /(coinbase|binance|kraken|okx|kucoin|gate\.io|gate io|bybit|mexc|bilaxy|deribit|bitfinex|bitstamp|gemini|huobi|ascendex|upbit|bithumb|crypto\.com|cryptocom|lbank|poloniex|bitget)/.test(
      text,
    )
  ) {
    return "cex";
  }

  if (text.includes("router") || text.includes("aggregator")) {
    return "router";
  }

  if (text.includes("bridge")) {
    return "bridge";
  }

  if (text.includes("dex") || text.includes("swap") || text.includes("amm") || text.includes("exchange")) {
    return "dex";
  }

  if (text.includes("vault") || text.includes("staking") || text.includes("validator")) {
    return "staking";
  }

  if (text.includes("token")) {
    return "token";
  }

  if (text.includes("contract") || text.includes("proxy") || text.includes("implementation")) {
    return "contract";
  }

  if (text.includes("protocol") || text.includes("defi") || text.includes("dao")) {
    return "protocol";
  }

  return null;
};

const ethLabelsEntityType = (category: string | null) => {
  if (category === "cex") {
    return "exchange";
  }

  if (category === "contract" || category === "router" || category === "bridge" || category === "dex") {
    return "contract";
  }

  return null;
};

const ethLabelsRank = (label: string, category: string | null) => {
  const baseRank =
    category === "cex"
      ? 0
      : category === "router"
        ? 1
        : category === "bridge"
          ? 2
          : category === "dex"
            ? 3
            : category === "protocol"
              ? 4
              : category === "staking"
                ? 5
                : category === "contract"
                  ? 6
                  : category === "token"
                    ? 7
                    : 8;

  return baseRank * 100 + label.length;
};

const loadEthLabels = async () => {
  const sourcePath = vendoredPath("eth_labels_accounts.csv");
  const sourceRecordedAt = await parseSourceRecordedAt(sourcePath);
  const rows = parseRows(await readTextFile(sourcePath), ",");
  const selected = rows.reduce<Map<string, { rank: number; row: RawAddressLabelRow }>>((result, row) => {
    const network = normalizeNetwork(ethLabelsChainMap[row.chain_id || ""]);
    const address = normalizeAddress(row.address);
    const slug = cleanText(row.label)?.toLowerCase();
    const nameTag = cleanText(row.name_tag);
    const label = nameTag || slug;

    if (!network || !address || !slug || !label || excludedEthLabels.has(slug)) {
      return result;
    }

    const category = ethLabelsCategory(slug, nameTag);
    const key = `${network}:${address}`;
    const rank = ethLabelsRank(slug, category);
    const prior = result.get(key);

    if (!prior || rank < prior.rank) {
      result.set(
        key,
        {
          rank,
          row: buildRawLabel({
            network,
            address,
            sourceName: "eth_labels",
            sourceType: "file",
            sourceUri: sourcePath,
            label,
            category,
            entityType: ethLabelsEntityType(category),
            aggregatorLabel: slug,
            aggregatorSource: "eth-labels",
            upstreamAggregatorLabel: null,
            upstreamAggregatorSource: null,
            confidence: 0.55,
            metadata: row,
            sourceRecordedAt,
            externalAddedAt: cleanText(row.updated_at) ? new Date(String(row.updated_at)) : null,
          }),
        },
      );
    }

    return result;
  }, new Map());

  return [...selected.values()].map((entry) => entry.row);
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

const loadCuratedCsvLabels = async ({
  fileName,
  sourceName,
  sourceType,
}: {
  fileName: string;
  sourceName: string;
  sourceType: string;
}) => {
  const sourcePath = vendoredPath(fileName);
  const sourceRecordedAt = await parseSourceRecordedAt(sourcePath);
  const rows = parseRows(await readTextFile(sourcePath), ",");

  return rows.flatMap((row) => {
    const network = normalizeNetwork(cleanText(row.network));
    const address = normalizeAddress(row.address);
    const label = cleanText(row.label);
    const confidence = Number.parseFloat(cleanText(row.confidence) || "");

    return network && address && label
      ? [
          buildRawLabel({
            network,
            address,
            sourceKey: cleanText(row.source_key) || `${network}:${address}`,
            sourceName,
            sourceType,
            sourceUri: cleanText(row.source_uri),
            label,
            category: cleanText(row.category),
            entityType: cleanText(row.entity_type),
            aggregatorLabel: cleanText(row.aggregator_label),
            aggregatorSource: cleanText(row.aggregator_source),
            upstreamAggregatorLabel: null,
            upstreamAggregatorSource: null,
            confidence: Number.isFinite(confidence) ? confidence : 0.9,
            metadata: row,
            sourceRecordedAt,
            externalAddedAt: cleanText(row.external_added_at) ? new Date(String(row.external_added_at)) : null,
          }),
        ]
      : [];
  });
};

const loadFirstPartyExchangeLabels = () =>
  loadCuratedCsvLabels({
    fileName: "first_party_exchange_labels.csv",
    sourceName: "first_party_exchange_labels",
    sourceType: "first_party_exchange",
  });

const loadInternetManualLabels = () =>
  loadCuratedCsvLabels({
    fileName: "internet_manual_labels.csv",
    sourceName: "internet_manual_labels",
    sourceType: "manual_web",
  });

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

const duneBlockchainFromNetwork = (network: string) =>
  ({
    eth: "ethereum",
    base: "base",
    polygon: "polygon",
    arbitrum: "arbitrum",
    bsc: "bnb",
    optimism: "optimism",
    avalanche: "avalanche_c",
    zora: "zora",
    unichain: "unichain",
    blast: "blast",
    ink: "ink",
  })[network] || null;

const resolveDuneUploadNamespace = () =>
  cleanText(parseArgValue("dune-upload-namespace")) || cleanText(process.env.DUNE_UPLOAD_NAMESPACE);

const listUnlabeledFlowAddresses = async (client: ReturnType<typeof createServingClient>) => {
  const result = await client.query<{ network: string; address: string }>(`
    with flow_addresses as (
      select distinct network, address
      from token_daily_address_flows
    )
    select flow_addresses.network, flow_addresses.address
    from flow_addresses
    left join address_labels
      on address_labels.network = flow_addresses.network
     and address_labels.address = flow_addresses.address
    where address_labels.address is null
    order by flow_addresses.network, flow_addresses.address
  `);

  return result.rows.flatMap((row) => {
    const blockchain = duneBlockchainFromNetwork(row.network);
    return blockchain ? [{ ...row, blockchain }] : [];
  });
};

const csvValue = (value: string) => `"${value.replaceAll('"', '""')}"`;

const buildUnlabeledAddressCsv = (rows: Array<{ network: string; blockchain: string; address: string }>) =>
  ["network,blockchain,address", ...rows.map((row) => [row.network, row.blockchain, row.address].map(csvValue).join(","))].join(
    "\n",
  );

const uploadedAddressSchema = [
  { name: "network", type: "varchar" },
  { name: "blockchain", type: "varchar" },
  { name: "address", type: "varbinary" },
];

const waitForUploadedTableRows = async ({
  fullName,
  expectedRows,
}: {
  fullName: string;
  expectedRows: number;
}) => {
  if (expectedRows <= 0) {
    return;
  }

  const maxAttempts = 60;
  const delayMs = 5000;

  for (const attempt of Array.from({ length: maxAttempts }, (_, index) => index + 1)) {
    const [row] = await executeDuneSql<{ rows: number }>({
      sql: `select count(*) as rows from ${fullName}`,
    });
    const currentRows = Number(row?.rows || 0);

    if (currentRows >= expectedRows) {
      logLine("confirmed uploaded Dune table row count", {
        table_name: fullName,
        rows: currentRows,
        expected_rows: expectedRows,
        attempt,
      });
      return;
    }

    logLine("waiting for uploaded Dune table row count", {
      table_name: fullName,
      rows: currentRows,
      expected_rows: expectedRows,
      attempt,
    });
    await sleep(delayMs);
  }

  throw new Error(`uploaded Dune table ${fullName} did not reach ${expectedRows} rows before timeout`);
};

const uploadedDuneSourceSql = {
  dune_uploaded_owner_labels: (tableName: string) => `
    with uploaded as (
      select network, blockchain, address
      from ${tableName}
    ),
    ranked as (
      select
        uploaded.network,
        concat('0x', lower(to_hex(uploaded.address))) as address,
        coalesce(details.name, owner.contract_name, owner.custody_owner, owner.account_owner, owner.owner_key) as label,
        coalesce(
          details.primary_category,
          case when owner.contract_name is not null then 'contract' end,
          case when owner.custody_owner is not null then 'custody' end,
          case when owner.account_owner is not null then 'owner' end
        ) as category,
        case when owner.contract_name is not null then 'contract' else 'owner' end as entity_type,
        coalesce(details.name, owner.custody_owner, owner.account_owner, owner.owner_key) as aggregator_label,
        coalesce(owner.source, 'labels.owner_addresses') as aggregator_source,
        coalesce(details.updated_at, details.created_at, owner.updated_at, owner.created_at) as external_added_at,
        row_number() over (
          partition by uploaded.network, uploaded.address
          order by
            case
              when details.primary_category is not null then 0
              when owner.contract_name is not null then 1
              when owner.custody_owner is not null then 2
              when owner.account_owner is not null then 3
              else 4
            end,
            coalesce(details.updated_at, details.created_at, owner.updated_at, owner.created_at) desc,
            owner.owner_key asc
        ) as row_number
      from uploaded
      inner join labels.owner_addresses as owner
        on owner.blockchain = uploaded.blockchain
       and owner.address = uploaded.address
      left join labels.owner_details as details
        on details.owner_key = owner.owner_key
    )
    select
      network,
      address,
      label,
      category,
      entity_type,
      aggregator_label,
      aggregator_source,
      external_added_at
    from ranked
    where row_number = 1
      and label is not null
      and label <> ''
  `,
  dune_uploaded_identifier_labels: (tableName: string) => `
    with uploaded as (
      select network, blockchain, address
      from ${tableName}
    ),
    ranked as (
      select
        uploaded.network,
        concat('0x', lower(to_hex(uploaded.address))) as address,
        labels.name as label,
        labels.category as category,
        case
          when labels.category = 'institution' then 'organization'
          when labels.category like '%contract%' or labels.model_name = 'contracts' then 'contract'
          else null
        end as entity_type,
        labels.model_name as aggregator_label,
        'labels.addresses' as aggregator_source,
        coalesce(labels.updated_at, labels.created_at) as external_added_at,
        row_number() over (
          partition by uploaded.network, uploaded.address
          order by
            case
              when labels.category = 'institution' then 0
              when labels.category = 'bridge' then 1
              when labels.category = 'dao' then 2
              when labels.category = 'project wallet' then 3
              when labels.category = 'infrastructure' then 4
              when labels.category = 'contracts' then 5
              else 20
            end,
            coalesce(labels.updated_at, labels.created_at) desc,
            labels.model_name asc,
            labels.name asc
        ) as row_number
      from uploaded
      inner join labels.addresses as labels
        on labels.blockchain = uploaded.blockchain
       and labels.address = uploaded.address
      where labels.label_type = 'identifier'
        and labels.category <> 'social'
    )
    select
      network,
      address,
      label,
      category,
      entity_type,
      aggregator_label,
      aggregator_source,
      external_added_at
    from ranked
    where row_number = 1
      and label is not null
      and label <> ''
  `,
  dune_uploaded_ens_labels: (tableName: string) => `
    with uploaded as (
      select network, blockchain, address
      from ${tableName}
    ),
    ranked as (
      select
        uploaded.network,
        concat('0x', lower(to_hex(uploaded.address))) as address,
        ens.name as label,
        'ens' as category,
        null as entity_type,
        'ens' as aggregator_label,
        'labels.ens' as aggregator_source,
        coalesce(ens.updated_at, ens.created_at) as external_added_at,
        row_number() over (
          partition by uploaded.network, uploaded.address
          order by coalesce(ens.updated_at, ens.created_at) desc, ens.name asc
        ) as row_number
      from uploaded
      inner join labels.ens as ens
        on ens.blockchain = uploaded.blockchain
       and ens.address = uploaded.address
    )
    select
      network,
      address,
      label,
      category,
      entity_type,
      aggregator_label,
      aggregator_source,
      external_added_at
    from ranked
    where row_number = 1
      and label is not null
      and label <> ''
  `,
} as const;

const loadUploadedDuneRows = async ({
  namespace,
  client,
}: {
  namespace: string | null;
  client: ReturnType<typeof createServingClient>;
}) => {
  if (!namespace) {
    logLine("skipped uploaded Dune label sync", { reason: "missing_dune_upload_namespace" });
    return [] as RawAddressLabelRow[];
  }

  const unlabeled = await listUnlabeledFlowAddresses(client);

  if (unlabeled.length === 0) {
    logLine("skipped uploaded Dune label sync", { reason: "no_unlabeled_flow_addresses" });
    return [] as RawAddressLabelRow[];
  }

  const uploadedTable = await ensureUploadedTable({
    namespace,
    tableName: uploadedDuneTableName,
    description: uploadedDuneTableDescription,
    schema: uploadedAddressSchema,
  });
  const fullName = normalizeUploadedTableName(uploadedTable.full_name);
  const csv = buildUnlabeledAddressCsv(unlabeled);

  await clearUploadedTable({ namespace: uploadedTable.namespace, tableName: uploadedTable.table_name });
  const insertResult = await insertUploadedCsv({
    namespace: uploadedTable.namespace,
    tableName: uploadedTable.table_name,
    csv,
  });
  logLine("uploaded unlabeled flow addresses to Dune", {
    table_name: fullName,
    rows: insertResult.rows_written,
    bytes: insertResult.bytes_written,
  });
  await waitForUploadedTableRows({ fullName, expectedRows: insertResult.rows_written });

  const sourceRecordedAt = new Date();
  const groups = await Promise.all(
    Object.entries(uploadedDuneSourceSql).map(async ([sourceName, sqlBuilder]) => {
      const rows = await executeDuneSql<Record<string, unknown>>({ sql: sqlBuilder(fullName) });
      logLine("synced uploaded Dune source", { source_name: sourceName, rows: rows.length });

      return rows.flatMap((row) => {
        const network = normalizeNetwork(cleanText(row.network));
        const address = normalizeAddress(cleanText(row.address));
        const label = cleanText(row.label);

        return network && address && label
          ? [
              buildRawLabel({
                network,
                address,
                sourceName,
                sourceType: "dune_upload_join",
                sourceUri: fullName,
                label,
                category: cleanText(row.category),
                entityType: cleanText(row.entity_type),
                aggregatorLabel: cleanText(row.aggregator_label),
                aggregatorSource: cleanText(row.aggregator_source),
                upstreamAggregatorLabel: null,
                upstreamAggregatorSource: null,
                confidence:
                  sourceName === "dune_uploaded_owner_labels"
                    ? 0.8
                    : sourceName === "dune_uploaded_identifier_labels"
                      ? 0.72
                      : 0.5,
                metadata: row,
                sourceRecordedAt,
                externalAddedAt: cleanText(row.external_added_at) ? new Date(String(row.external_added_at)) : null,
              }),
            ]
          : [];
      });
    }),
  );

  return groups.flat();
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
  const skipDuneUpload = parseBool(parseArgValue("skip-dune-upload"));
  const duneUploadNamespace = resolveDuneUploadNamespace();
  const duneConfigPath = await resolvedDuneConfigPath();
  const startedAt = performance.now();
  const client = createServingClient();
  const sourceGroups = skipLocal
      ? []
      : [
          { sourceName: "eigen_manual_labels", rows: await loadEigenManualLabels() },
          { sourceName: "first_party_exchange_labels", rows: await loadFirstPartyExchangeLabels() },
          { sourceName: "internet_manual_labels", rows: await loadInternetManualLabels() },
          { sourceName: "eigen_cex_labels", rows: await loadEigenCexLabels() },
          { sourceName: "eigen_etherscan_labels", rows: await loadEigenEtherscanLabels() },
          { sourceName: "eth_labels", rows: await loadEthLabels() },
        { sourceName: "eigen_names", rows: await loadEigenNames() },
        { sourceName: "superset_cr_router_labels", rows: await loadSupersetRouterLabels() },
      ];
  const duneRows = await loadDuneSources(duneConfigPath);
  const duneGroups = groupRowsBySourceName(duneRows);

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

    const uploadedDuneRows = skipDuneUpload ? [] : await loadUploadedDuneRows({ namespace: duneUploadNamespace, client });
    const uploadedDuneGroups = groupRowsBySourceName(uploadedDuneRows);

    if (uploadedDuneRows.length > 0) {
      await client.query("begin");

      for (const [sourceName, rows] of Object.entries(uploadedDuneGroups)) {
        const deletedRows = await deleteMissingSnapshotRows({
          client,
          sourceName,
          currentRows: rows,
        });
        await upsertRawRows(client, rows);
        logLine("synced uploaded Dune snapshot", {
          source_name: sourceName,
          rows: rows.length,
          deleted_rows: deletedRows,
        });
      }

      await client.query("truncate table address_labels");
      await client.query(resolveLabelsSql);
      await client.query("commit");
    }

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
