import { createServingClient, isDirectRun, normalizeAddress, normalizeNetwork, parseArgValue } from "./serving.js";
import { ensureServingSchema } from "./serving-schema.js";
import { logLine } from "./log.js";

const enrichedInsertSql = `
  with priced_flows as (
    select
      leaderboards.network,
      leaderboards.token_address,
      leaderboards.window_days,
      leaderboards.metric,
      leaderboards.address,
      case
        when leaderboards.metric = 'net_inflow' then sum(flows.net_amount_native_sum * prices.median_price_usd)
        when leaderboards.metric = 'net_outflow' then -sum(flows.net_amount_native_sum * prices.median_price_usd)
        else null
      end as amount_usd_sum
    from token_flow_leaderboards as leaderboards
    inner join token_daily_address_flows as flows
      on flows.network = leaderboards.network
     and flows.token_address = leaderboards.token_address
     and flows.address = leaderboards.address
     and flows.day between leaderboards.window_start_day and leaderboards.window_end_day
    left join token_price_daily as prices
      on prices.network = flows.network
     and prices.token_address = flows.token_address
     and prices.day = flows.day
    group by 1, 2, 3, 4, 5
  )
  insert into token_flow_leaderboards_enriched (
    network,
    token_address,
    token_name,
    token_symbol,
    token_decimals,
    target_source,
    coingecko_id,
    coingecko_name,
    coingecko_symbol,
    window_days,
    window_start_day,
    window_end_day,
    is_partial_day,
    metric,
    address,
    flow_rank,
    transfer_count,
    amount_native_sum,
    amount_usd_sum,
    avg_amount_native,
    avg_amount_usd,
    label,
    category,
    entity_type,
    aggregator_label,
    aggregator_source,
    upstream_aggregator_label,
    upstream_aggregator_source,
    label_source_name,
    label_source_key,
    label_confidence,
    label_source_recorded_at,
    label_external_added_at,
    as_of_ts
  )
  select
    leaderboards.network,
    leaderboards.token_address,
    leaderboards.token_name,
    leaderboards.token_symbol,
    leaderboards.token_decimals,
    leaderboards.target_source,
    leaderboards.coingecko_id,
    leaderboards.coingecko_name,
    leaderboards.coingecko_symbol,
    leaderboards.window_days,
    leaderboards.window_start_day,
    leaderboards.window_end_day,
    leaderboards.is_partial_day,
    leaderboards.metric,
    leaderboards.address,
    leaderboards.flow_rank,
    leaderboards.transfer_count,
    leaderboards.amount_native_sum,
    priced_flows.amount_usd_sum,
    leaderboards.avg_amount_native,
    case
      when leaderboards.transfer_count = 0 or priced_flows.amount_usd_sum is null then null
      else priced_flows.amount_usd_sum / leaderboards.transfer_count
    end as avg_amount_usd,
    labels.label,
    labels.category,
    labels.entity_type,
    labels.aggregator_label,
    labels.aggregator_source,
    labels.upstream_aggregator_label,
    labels.upstream_aggregator_source,
    labels.source_name,
    labels.source_key,
    labels.confidence,
    labels.source_recorded_at,
    labels.external_added_at,
    leaderboards.as_of_ts
  from token_flow_leaderboards as leaderboards
  left join priced_flows
    on priced_flows.network = leaderboards.network
   and priced_flows.token_address = leaderboards.token_address
   and priced_flows.window_days = leaderboards.window_days
   and priced_flows.metric = leaderboards.metric
   and priced_flows.address = leaderboards.address
  left join address_labels as labels
    on labels.network = leaderboards.network
   and labels.address = leaderboards.address
`;

const scopedEnrichedInsertSql = `
  with scoped_leaderboards as (
    select *
    from token_flow_leaderboards
    where ($1::text is null or network = $1)
      and ($2::text is null or token_address = $2)
  ),
  priced_flows as (
    select
      leaderboards.network,
      leaderboards.token_address,
      leaderboards.window_days,
      leaderboards.metric,
      leaderboards.address,
      case
        when leaderboards.metric = 'net_inflow' then sum(flows.net_amount_native_sum * prices.median_price_usd)
        when leaderboards.metric = 'net_outflow' then -sum(flows.net_amount_native_sum * prices.median_price_usd)
        else null
      end as amount_usd_sum
    from scoped_leaderboards as leaderboards
    inner join token_daily_address_flows as flows
      on flows.network = leaderboards.network
     and flows.token_address = leaderboards.token_address
     and flows.address = leaderboards.address
     and flows.day between leaderboards.window_start_day and leaderboards.window_end_day
    left join token_price_daily as prices
      on prices.network = flows.network
     and prices.token_address = flows.token_address
     and prices.day = flows.day
    group by 1, 2, 3, 4, 5
  )
  insert into token_flow_leaderboards_enriched (
    network,
    token_address,
    token_name,
    token_symbol,
    token_decimals,
    target_source,
    coingecko_id,
    coingecko_name,
    coingecko_symbol,
    window_days,
    window_start_day,
    window_end_day,
    is_partial_day,
    metric,
    address,
    flow_rank,
    transfer_count,
    amount_native_sum,
    amount_usd_sum,
    avg_amount_native,
    avg_amount_usd,
    label,
    category,
    entity_type,
    aggregator_label,
    aggregator_source,
    upstream_aggregator_label,
    upstream_aggregator_source,
    label_source_name,
    label_source_key,
    label_confidence,
    label_source_recorded_at,
    label_external_added_at,
    as_of_ts
  )
  select
    leaderboards.network,
    leaderboards.token_address,
    leaderboards.token_name,
    leaderboards.token_symbol,
    leaderboards.token_decimals,
    leaderboards.target_source,
    leaderboards.coingecko_id,
    leaderboards.coingecko_name,
    leaderboards.coingecko_symbol,
    leaderboards.window_days,
    leaderboards.window_start_day,
    leaderboards.window_end_day,
    leaderboards.is_partial_day,
    leaderboards.metric,
    leaderboards.address,
    leaderboards.flow_rank,
    leaderboards.transfer_count,
    leaderboards.amount_native_sum,
    priced_flows.amount_usd_sum,
    leaderboards.avg_amount_native,
    case
      when leaderboards.transfer_count = 0 or priced_flows.amount_usd_sum is null then null
      else priced_flows.amount_usd_sum / leaderboards.transfer_count
    end as avg_amount_usd,
    labels.label,
    labels.category,
    labels.entity_type,
    labels.aggregator_label,
    labels.aggregator_source,
    labels.upstream_aggregator_label,
    labels.upstream_aggregator_source,
    labels.source_name,
    labels.source_key,
    labels.confidence,
    labels.source_recorded_at,
    labels.external_added_at,
    leaderboards.as_of_ts
  from scoped_leaderboards as leaderboards
  left join priced_flows
    on priced_flows.network = leaderboards.network
   and priced_flows.token_address = leaderboards.token_address
   and priced_flows.window_days = leaderboards.window_days
   and priced_flows.metric = leaderboards.metric
   and priced_flows.address = leaderboards.address
  left join address_labels as labels
    on labels.network = leaderboards.network
   and labels.address = leaderboards.address
`;

export const refreshEnrichedLeaderboards = async ({
  network = normalizeNetwork(parseArgValue("network")),
  tokenAddress = normalizeAddress(parseArgValue("token-address")),
}: {
  network?: string | null;
  tokenAddress?: string | null;
} = {}) => {
  await ensureServingSchema();
  const client = createServingClient();
  const startedAt = performance.now();
  await client.connect();

  try {
    const hasDailyFlows = await client.query<{ has_rows: boolean }>(`
      select exists (
        select 1
        from token_daily_address_flows
        limit 1
      ) as has_rows
    `);

    if (!hasDailyFlows.rows[0]?.has_rows) {
      logLine("skipped enriched leaderboard refresh", { reason: "token_daily_address_flows_is_empty" });
      return;
    }

    await client.query("begin");
    if (network || tokenAddress) {
      await client.query(
        `
          delete from token_flow_leaderboards_enriched
          where ($1::text is null or network = $1)
            and ($2::text is null or token_address = $2)
        `,
        [network, tokenAddress],
      );
      await client.query(scopedEnrichedInsertSql, [network, tokenAddress]);
    } else {
      await client.query("truncate table token_flow_leaderboards_enriched");
      await client.query(enrichedInsertSql);
    }
    await client.query("commit");

    const result = await client.query<{ count: string }>(
      `
        select count(*)::text as count
        from token_flow_leaderboards_enriched
        where ($1::text is null or network = $1)
          and ($2::text is null or token_address = $2)
      `,
      [network, tokenAddress],
    );
    logLine("completed enriched leaderboard refresh", {
      network: network ?? undefined,
      token_address: tokenAddress ?? undefined,
      rows: Number(result.rows[0]?.count || 0),
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
  refreshEnrichedLeaderboards().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
