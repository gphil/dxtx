import { createServingClient, parseArgValue, parseIntegerArg } from "./serving.js";
import { logLine } from "./log.js";

type CountRow = {
  count: string | number;
};

type PriceCoverageRow = {
  gap_days: string | number;
  gap_tokens: string | number;
  earliest_gap_day: string | null;
  latest_gap_day: string | null;
};

type PriceGapRow = {
  network: string;
  token_address: string;
  coingecko_id: string;
  gap_days: string | number;
  earliest_gap_day: string;
  latest_gap_day: string;
};

type EnrichedConsistencyRow = {
  checked_rows: string | number;
  amount_bad_rows: string | number;
  avg_bad_rows: string | number;
  max_amount_error: string | number | null;
  max_avg_error: string | number | null;
};

type EnrichedBadSampleRow = {
  network: string;
  token_address: string;
  metric: string;
  window_days: string | number;
  flow_rank: string | number;
  address: string;
  window_end_day: string;
  reference_price_usd: string | number;
  implied_price_usd: string | number;
  relative_error: string | number;
};

type EnrichedMissingUsdRow = {
  rows: string | number;
  tokens: string | number;
};

type WethSpotRow = {
  network: string;
  token_address: string;
  coingecko_id: string | null;
  price_day: string | null;
  reference_price_usd: string | number | null;
  sample_rows: string | number;
  implied_price_min: string | number | null;
  implied_price_max: string | number | null;
};

const currentTokenMapSql = `
  select distinct on (network, token_address)
    network,
    token_address,
    coingecko_id
  from token_flow_leaderboards
  where coingecko_id is not null
    and coingecko_id <> ''
  order by network, token_address, as_of_ts desc, window_end_day desc, flow_rank asc, coingecko_id desc
`;

const priceGapsSql = `
  with token_map as (
    ${currentTokenMapSql}
  ),
  flow_bounds as (
    select
      network,
      token_address,
      greatest(min(day), $1::date) as first_flow_day,
      max(day) as last_flow_day
    from token_flow_daily_totals
    where day >= $1::date
    group by 1, 2
  ),
  expected_days as (
    select
      bounds.network,
      bounds.token_address,
      token_map.coingecko_id,
      generated.day::date as day
    from flow_bounds as bounds
    inner join token_map
      on token_map.network = bounds.network
     and token_map.token_address = bounds.token_address
    cross join lateral generate_series(bounds.first_flow_day, bounds.last_flow_day, interval '1 day') as generated(day)
  )
  select
    expected_days.network,
    expected_days.token_address,
    expected_days.coingecko_id,
    count(*)::text as gap_days,
    min(expected_days.day)::text as earliest_gap_day,
    max(expected_days.day)::text as latest_gap_day
  from expected_days
  left join token_price_daily as prices
    on prices.network = expected_days.network
   and prices.token_address = expected_days.token_address
   and prices.day = expected_days.day
  where prices.day is null
     or prices.coingecko_id <> expected_days.coingecko_id
  group by 1, 2, 3
`;

const priceCoverageSql = `
  with gaps as (
    ${priceGapsSql}
  )
  select
    coalesce(sum(gap_days::bigint), 0)::text as gap_days,
    count(*)::text as gap_tokens,
    min(earliest_gap_day) as earliest_gap_day,
    max(latest_gap_day) as latest_gap_day
  from gaps
`;

const topPriceGapsSql = `
  with gaps as (
    ${priceGapsSql}
  )
  select *
  from gaps
  order by gap_days::bigint desc, network, token_address
  limit $2::integer
`;

const priceIdMismatchSql = `
  with token_map as (
    ${currentTokenMapSql}
  )
  select count(*)::text as count
  from token_price_daily as prices
  inner join token_map
    on token_map.network = prices.network
   and token_map.token_address = prices.token_address
  where prices.coingecko_id <> token_map.coingecko_id
`;

const enrichedConsistencySql = `
  with checked as (
    select
      enriched.*,
      latest_price.median_price_usd as reference_price_usd,
      abs((abs(enriched.amount_usd_sum / nullif(enriched.amount_native_sum, 0)) - latest_price.median_price_usd)
        / nullif(latest_price.median_price_usd, 0)) as amount_relative_error,
      abs((abs(enriched.avg_amount_usd / nullif(enriched.avg_amount_native, 0)) - latest_price.median_price_usd)
        / nullif(latest_price.median_price_usd, 0)) as avg_relative_error
    from token_flow_leaderboards_enriched as enriched
    inner join lateral (
      select prices.median_price_usd
      from token_price_daily as prices
      where prices.network = enriched.network
        and prices.token_address = enriched.token_address
        and prices.day <= enriched.window_end_day
      order by prices.day desc
      limit 1
    ) as latest_price on true
    where enriched.metric in ('net_inflow', 'net_outflow')
      and enriched.amount_native_sum <> 0
      and enriched.avg_amount_native <> 0
      and enriched.amount_usd_sum is not null
      and enriched.avg_amount_usd is not null
  )
  select
    count(*)::text as checked_rows,
    count(*) filter (where amount_relative_error > $1::double precision)::text as amount_bad_rows,
    count(*) filter (where avg_relative_error > $1::double precision)::text as avg_bad_rows,
    max(amount_relative_error)::text as max_amount_error,
    max(avg_relative_error)::text as max_avg_error
  from checked
`;

const enrichedBadSamplesSql = `
  with checked as (
    select
      enriched.network,
      enriched.token_address,
      enriched.metric,
      enriched.window_days,
      enriched.flow_rank,
      enriched.address,
      enriched.window_end_day::text as window_end_day,
      latest_price.median_price_usd as reference_price_usd,
      abs(enriched.amount_usd_sum / nullif(enriched.amount_native_sum, 0)) as implied_price_usd,
      abs((abs(enriched.amount_usd_sum / nullif(enriched.amount_native_sum, 0)) - latest_price.median_price_usd)
        / nullif(latest_price.median_price_usd, 0)) as relative_error
    from token_flow_leaderboards_enriched as enriched
    inner join lateral (
      select prices.median_price_usd
      from token_price_daily as prices
      where prices.network = enriched.network
        and prices.token_address = enriched.token_address
        and prices.day <= enriched.window_end_day
      order by prices.day desc
      limit 1
    ) as latest_price on true
    where enriched.metric in ('net_inflow', 'net_outflow')
      and enriched.amount_native_sum <> 0
      and enriched.amount_usd_sum is not null
  )
  select *
  from checked
  where relative_error > $1::double precision
  order by relative_error desc
  limit $2::integer
`;

const enrichedMissingUsdSql = `
  select
    count(*)::text as rows,
    count(distinct network || ':' || token_address)::text as tokens
  from token_flow_leaderboards_enriched
  where metric in ('net_inflow', 'net_outflow')
    and (amount_usd_sum is null or avg_amount_usd is null)
`;

const wethSpotSql = `
  with weth(network, token_address) as (
    values
      ('eth', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'),
      ('optimism', '0x4200000000000000000000000000000000000006'),
      ('base', '0x4200000000000000000000000000000000000006'),
      ('polygon', '0x7ceb23fd6bc0add59e62ac25578270cff1b9f619'),
      ('bsc', '0x2170ed0880ac9a755fd29b2688956bd959f933f8'),
      ('unichain', '0x4200000000000000000000000000000000000006'),
      ('avalanche', '0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab'),
      ('arbitrum', '0x82af49447d8a07e3bd95bd0d56f35241523fbab1')
  ),
  latest_prices as (
    select distinct on (prices.network, prices.token_address)
      prices.network,
      prices.token_address,
      prices.coingecko_id,
      prices.day::text as price_day,
      prices.median_price_usd as reference_price_usd
    from token_price_daily as prices
    inner join weth
      on weth.network = prices.network
     and weth.token_address = prices.token_address
    order by prices.network, prices.token_address, prices.day desc
  ),
  implied_prices as (
    select
      enriched.network,
      enriched.token_address,
      count(*) as sample_rows,
      min(abs(enriched.amount_usd_sum / nullif(enriched.amount_native_sum, 0))) as implied_price_min,
      max(abs(enriched.amount_usd_sum / nullif(enriched.amount_native_sum, 0))) as implied_price_max
    from token_flow_leaderboards_enriched as enriched
    inner join weth
      on weth.network = enriched.network
     and weth.token_address = enriched.token_address
    where enriched.metric in ('net_inflow', 'net_outflow')
      and enriched.window_days = 7
      and enriched.amount_native_sum <> 0
      and enriched.amount_usd_sum is not null
    group by 1, 2
  )
  select
    weth.network,
    weth.token_address,
    latest_prices.coingecko_id,
    latest_prices.price_day,
    latest_prices.reference_price_usd,
    coalesce(implied_prices.sample_rows, 0)::text as sample_rows,
    implied_prices.implied_price_min,
    implied_prices.implied_price_max
  from weth
  left join latest_prices
    on latest_prices.network = weth.network
   and latest_prices.token_address = weth.token_address
  left join implied_prices
    on implied_prices.network = weth.network
   and implied_prices.token_address = weth.token_address
  order by weth.network
`;

const toNumber = (value: string | number | null | undefined) => Number(value ?? 0);
const formatNumber = (value: string | number | null | undefined, digits = 4) => {
  if (value === null || value === undefined) {
    return undefined;
  }

  const number = toNumber(value);
  return Number.isFinite(number) ? number.toFixed(digits) : undefined;
};

const logRows = <T>(message: string, rows: T[], fields: (row: T) => Record<string, string | number | undefined>) =>
  rows.forEach((row) => logLine(message, fields(row)));

const main = async () => {
  const client = createServingClient();
  const startDay = parseArgValue("start-day") ?? "2026-01-01";
  const sampleLimit = parseIntegerArg("sample-limit") ?? 10;
  const thresholdBps = parseIntegerArg("threshold-bps") ?? 100;
  const threshold = thresholdBps / 10_000;
  const startedAt = performance.now();

  await client.connect();

  try {
    const coverage = await client.query<PriceCoverageRow>(priceCoverageSql, [startDay]);
    const priceMismatches = await client.query<CountRow>(priceIdMismatchSql);
    const enrichedConsistency = await client.query<EnrichedConsistencyRow>(enrichedConsistencySql, [threshold]);
    const enrichedMissingUsd = await client.query<EnrichedMissingUsdRow>(enrichedMissingUsdSql);
    const topGaps = await client.query<PriceGapRow>(topPriceGapsSql, [startDay, sampleLimit]);
    const badSamples = await client.query<EnrichedBadSampleRow>(enrichedBadSamplesSql, [threshold, sampleLimit]);
    const wethSpot = await client.query<WethSpotRow>(wethSpotSql);

    const coverageRow = coverage.rows[0];
    const consistencyRow = enrichedConsistency.rows[0];
    const missingUsdRow = enrichedMissingUsd.rows[0];
    const gapDays = toNumber(coverageRow?.gap_days);
    const gapTokens = toNumber(coverageRow?.gap_tokens);
    const mismatchRows = toNumber(priceMismatches.rows[0]?.count);
    const amountBadRows = toNumber(consistencyRow?.amount_bad_rows);
    const avgBadRows = toNumber(consistencyRow?.avg_bad_rows);

    logLine("checked price coverage", {
      start_day: startDay,
      gap_days: gapDays,
      gap_tokens: gapTokens,
      earliest_gap_day: coverageRow?.earliest_gap_day ?? undefined,
      latest_gap_day: coverageRow?.latest_gap_day ?? undefined,
    });
    logLine("checked price id consistency", {
      mismatched_price_rows: mismatchRows,
    });
    logLine("checked enriched usd consistency", {
      checked_rows: toNumber(consistencyRow?.checked_rows),
      threshold_bps: thresholdBps,
      amount_bad_rows: amountBadRows,
      avg_bad_rows: avgBadRows,
      max_amount_error_pct: formatNumber(toNumber(consistencyRow?.max_amount_error) * 100),
      max_avg_error_pct: formatNumber(toNumber(consistencyRow?.max_avg_error) * 100),
    });
    logLine("checked enriched missing usd", {
      rows: toNumber(missingUsdRow?.rows),
      tokens: toNumber(missingUsdRow?.tokens),
    });

    logRows("price gap sample", topGaps.rows, (row) => ({
      network: row.network,
      token_address: row.token_address,
      coingecko_id: row.coingecko_id,
      gap_days: toNumber(row.gap_days),
      earliest_gap_day: row.earliest_gap_day,
      latest_gap_day: row.latest_gap_day,
    }));
    logRows("enriched bad price sample", badSamples.rows, (row) => ({
      network: row.network,
      token_address: row.token_address,
      metric: row.metric,
      window_days: toNumber(row.window_days),
      flow_rank: toNumber(row.flow_rank),
      address: row.address,
      window_end_day: row.window_end_day,
      reference_price_usd: formatNumber(row.reference_price_usd),
      implied_price_usd: formatNumber(row.implied_price_usd),
      relative_error_pct: formatNumber(toNumber(row.relative_error) * 100),
    }));
    logRows("weth price spot check", wethSpot.rows, (row) => ({
      network: row.network,
      token_address: row.token_address,
      coingecko_id: row.coingecko_id ?? undefined,
      price_day: row.price_day ?? undefined,
      reference_price_usd: formatNumber(row.reference_price_usd),
      sample_rows: toNumber(row.sample_rows),
      implied_price_min: formatNumber(row.implied_price_min),
      implied_price_max: formatNumber(row.implied_price_max),
    }));

    const failed = gapDays > 0 || gapTokens > 0 || mismatchRows > 0 || amountBadRows > 0 || avgBadRows > 0;

    logLine(failed ? "price checks failed" : "price checks passed", {
      duration_ms: Math.round(performance.now() - startedAt),
    });

    if (failed) {
      process.exitCode = 1;
    }
  } finally {
    await client.end();
  }
};

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logLine("check prices failed", { error: message });
  process.exitCode = 1;
});
