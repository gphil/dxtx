import {
  addDays,
  buildUpsertSql,
  chunkRows,
  createServingClient,
  dayText,
  isDirectRun,
  median,
  nextDayText,
  normalizeAddress,
  normalizeNetwork,
  nowDayText,
  parseArgValue,
  parseIntegerArg,
} from "./serving.js";
import { ensureServingSchema } from "./serving-schema.js";
import { logLine } from "./log.js";

type PriceToken = {
  network: string;
  token_address: string;
  coingecko_id: string;
  first_flow_day: string;
  last_flow_day: string;
};

type PricePoint = [number, number];

type CoinGeckoRangeResponse = {
  prices?: PricePoint[];
};

type DailyPriceRow = {
  network: string;
  token_address: string;
  coingecko_id: string;
  day: string;
  median_price_usd: number;
  sample_count: number;
  source: string;
  is_intraday: boolean;
  first_price_ts: Date;
  last_price_ts: Date;
  updated_at: Date;
};

const maxRangeDays = 30;

const coingeckoBaseUrl = () =>
  process.env.COINGECKO_API_KEY ? "https://pro-api.coingecko.com/api/v3" : "https://api.coingecko.com/api/v3";

const coingeckoHeaders = () => ({
  Accept: "application/json",
  ...(process.env.COINGECKO_API_KEY ? { "x-cg-pro-api-key": process.env.COINGECKO_API_KEY } : {}),
});

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const priceRangeWindows = (days: string[]) => {
  if (days.length === 0) {
    return [] as Array<{ fromDay: string; toDay: string }>;
  }

  return days.slice(1).reduce(
    (result, day) => {
      const lastWindow = result[result.length - 1];

      if (!lastWindow) {
        return [{ fromDay: day, toDay: day }];
      }

      if (nextDayText(lastWindow.toDay) === day && addDays(lastWindow.fromDay, maxRangeDays - 1) >= day) {
        return [...result.slice(0, -1), { ...lastWindow, toDay: day }];
      }

      return [...result, { fromDay: day, toDay: day }];
    },
    [{ fromDay: days[0] || nowDayText(), toDay: days[0] || nowDayText() }],
  );
};

const fetchRangePrices = async ({
  coingeckoId,
  fromDay,
  toDay,
}: {
  coingeckoId: string;
  fromDay: string;
  toDay: string;
}) => {
  const fromTs = Math.floor(Date.parse(`${fromDay}T00:00:00Z`) / 1_000);
  const isTodayRange = toDay === nowDayText();
  const toTs = isTodayRange
    ? Math.floor(Date.now() / 1_000)
    : Math.floor(Date.parse(`${nextDayText(toDay)}T00:00:00Z`) / 1_000) - 1;
  const response = await fetch(
    `${coingeckoBaseUrl()}/coins/${encodeURIComponent(coingeckoId)}/market_chart/range?vs_currency=usd&from=${fromTs}&to=${toTs}`,
    { headers: coingeckoHeaders() },
  );

  if (!response.ok) {
    throw new Error(`CoinGecko range request failed status=${response.status} coingecko_id=${coingeckoId}`);
  }

  return (await response.json()) as CoinGeckoRangeResponse;
};

const buildDailyPriceRows = ({
  network,
  tokenAddress,
  coingeckoId,
  prices,
  currentDay,
}: {
  network: string;
  tokenAddress: string;
  coingeckoId: string;
  prices: PricePoint[];
  currentDay: string;
}) =>
  Object.entries(
    prices.reduce<Record<string, PricePoint[]>>((result, point) => {
      const day = dayText(new Date(point[0]));
      return { ...result, [day]: [...(result[day] || []), point] };
    }, {}),
  )
    .map(([day, dayPrices]) => {
      const medianPrice = median(dayPrices.map(([, price]) => price));

      if (medianPrice === null) {
        return null;
      }

      const firstPriceTs = dayPrices[0]?.[0];
      const lastPriceTs = dayPrices[dayPrices.length - 1]?.[0];

      if (firstPriceTs === undefined || lastPriceTs === undefined) {
        return null;
      }

      return {
        network,
        token_address: tokenAddress,
        coingecko_id: coingeckoId,
        day,
        median_price_usd: medianPrice,
        sample_count: dayPrices.length,
        source: "coingecko_market_chart_range",
        is_intraday: day === currentDay,
        first_price_ts: new Date(firstPriceTs),
        last_price_ts: new Date(lastPriceTs),
        updated_at: new Date(),
      } satisfies DailyPriceRow;
    })
    .filter((row): row is DailyPriceRow => row !== null);

const listActiveTokensSql = `
  with token_map as (
    select
      network,
      token_address,
      max(coingecko_id) as coingecko_id
    from token_flow_leaderboards
    where coingecko_id is not null
      and coingecko_id <> ''
    group by 1, 2
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
  )
  select
    bounds.network,
    bounds.token_address,
    token_map.coingecko_id,
    bounds.first_flow_day::text as first_flow_day,
    bounds.last_flow_day::text as last_flow_day
  from flow_bounds as bounds
  inner join token_map
    on token_map.network = bounds.network
   and token_map.token_address = bounds.token_address
  where bounds.last_flow_day >= current_date - ($2::integer * interval '1 day')
    and ($3::text is null or bounds.network = $3)
    and ($4::text is null or bounds.token_address = lower($4))
  order by bounds.last_flow_day desc, bounds.network, bounds.token_address
  limit coalesce($5::integer, 1000000)
`;

const listBackfillTokensSql = `
  with token_map as (
    select
      bounds.network,
      bounds.token_address,
      token_ids.coingecko_id,
      bounds.first_flow_day,
      bounds.last_flow_day
    from (
      select
        network,
        token_address,
        greatest(min(day), $1::date) as first_flow_day,
        max(day) as last_flow_day
      from token_flow_daily_totals
      where day >= $1::date
      group by 1, 2
    ) as bounds
    inner join (
      select
        network,
        token_address,
        max(coingecko_id) as coingecko_id
      from token_flow_leaderboards
      where coingecko_id is not null
        and coingecko_id <> ''
      group by 1, 2
    ) as token_ids
      on token_ids.network = bounds.network
     and token_ids.token_address = bounds.token_address
    where ($2::text is null or bounds.network = $2)
      and ($3::text is null or bounds.token_address = lower($3))
  ),
  expected_days as (
    select
      token_map.network,
      token_map.token_address,
      token_map.coingecko_id,
      token_map.first_flow_day,
      token_map.last_flow_day,
      generated.day::date as day
    from token_map
    cross join lateral generate_series(token_map.first_flow_day, token_map.last_flow_day, interval '1 day') as generated(day)
  ),
  missing_days as (
    select
      expected.network,
      expected.token_address,
      expected.coingecko_id,
      min(expected.day)::text as first_flow_day,
      max(expected.day)::text as last_flow_day
    from expected_days as expected
    left join token_price_daily as prices
      on prices.network = expected.network
     and prices.token_address = expected.token_address
     and prices.day = expected.day
    where prices.day is null
      and expected.day < current_date
    group by 1, 2, 3
  )
  select
    network,
    token_address,
    coingecko_id,
    first_flow_day,
    last_flow_day
  from missing_days
  order by first_flow_day, network, token_address
  limit $4
`;

const listMissingDaysSql = `
  with expected_days as (
    select generated.day::date as day
    from generate_series($3::date, $4::date, interval '1 day') as generated(day)
  )
  select expected_days.day::text as day
  from expected_days
  left join token_price_daily as prices
    on prices.network = $1
   and prices.token_address = $2
   and prices.day = expected_days.day
  where prices.day is null
  order by expected_days.day
`;

const upsertDailyPrices = async (rows: DailyPriceRow[]) => {
  if (rows.length === 0) {
    return 0;
  }

  const client = createServingClient();
  await client.connect();

  try {
    const columns = [
      "network",
      "token_address",
      "coingecko_id",
      "day",
      "median_price_usd",
      "sample_count",
      "source",
      "is_intraday",
      "first_price_ts",
      "last_price_ts",
      "updated_at",
    ];

    for (const chunk of chunkRows(rows, 1000)) {
      const { text, params } = buildUpsertSql({
        table: "token_price_daily",
        columns,
        rows: chunk as unknown as Record<string, unknown>[],
        conflict: ["network", "token_address", "day"],
        updates: columns.slice(2),
      });
      await client.query(text, params);
    }

    return rows.length;
  } finally {
    await client.end();
  }
};

const loadTokenWork = async ({
  startDay,
  activeDays,
  network,
  tokenAddress,
  limit,
  backfillLimit,
}: {
  startDay: string;
  activeDays: number;
  network: string | null;
  tokenAddress: string | null;
  limit: number | null;
  backfillLimit: number;
}) => {
  const client = createServingClient();
  await client.connect();

  try {
    const activeTokens = await client.query<PriceToken>(listActiveTokensSql, [
      startDay,
      activeDays,
      network,
      tokenAddress,
      limit,
    ]);
    const backfillTokens = await client.query<PriceToken>(listBackfillTokensSql, [
      startDay,
      network,
      tokenAddress,
      backfillLimit,
    ]);
    const byKey = [...activeTokens.rows, ...backfillTokens.rows].reduce<Record<string, PriceToken>>(
      (result, token) => ({
        ...result,
        [`${token.network}:${token.token_address}`]: result[`${token.network}:${token.token_address}`] || token,
      }),
      {},
    );

    return Object.values(byKey);
  } finally {
    await client.end();
  }
};

const loadMissingDays = async (token: PriceToken) => {
  const client = createServingClient();
  await client.connect();

  try {
    const result = await client.query<{ day: string }>(listMissingDaysSql, [
      token.network,
      token.token_address,
      token.first_flow_day,
      token.last_flow_day,
    ]);
    return result.rows.map((row) => row.day);
  } finally {
    await client.end();
  }
};

export const refreshPrices = async ({
  startDay = parseArgValue("start-day") ?? "2026-01-01",
  activeDays = parseIntegerArg("active-days") ?? 1,
  limit = parseIntegerArg("limit"),
  backfillLimit = parseIntegerArg("backfill-limit") ?? 50,
  network = normalizeNetwork(parseArgValue("network")),
  tokenAddress = normalizeAddress(parseArgValue("token-address")),
  requestDelayMs = parseIntegerArg("delay-ms") ?? 1250,
}: {
  startDay?: string;
  activeDays?: number;
  limit?: number | null;
  backfillLimit?: number;
  network?: string | null;
  tokenAddress?: string | null;
  requestDelayMs?: number;
} = {}) => {
  await ensureServingSchema();
  const startedAt = performance.now();
  const currentDay = nowDayText();
  const tokens = await loadTokenWork({
    startDay,
    activeDays,
    network,
    tokenAddress,
    limit,
    backfillLimit,
  });

  logLine("starting price refresh", {
    tokens: tokens.length,
    start_day: startDay,
    active_days: activeDays,
    backfill_limit: backfillLimit,
  });

  let upsertedRows = 0;

  for (const token of tokens) {
    const missingDays = await loadMissingDays(token);
    const requestedDays = [...missingDays, ...(token.last_flow_day >= currentDay ? [currentDay] : [])].filter(
      (day, index, values) =>
        values.indexOf(day) === index && day >= token.first_flow_day && day <= token.last_flow_day,
    );
    const windows = priceRangeWindows(requestedDays);

    if (windows.length === 0) {
      continue;
    }

    logLine("refreshing token prices", {
      network: token.network,
      token_address: token.token_address,
      coingecko_id: token.coingecko_id,
      windows: windows.length,
      from_day: windows[0]?.fromDay,
      to_day: windows[windows.length - 1]?.toDay,
    });

    for (const window of windows) {
      const rangeResponse = await fetchRangePrices({
        coingeckoId: token.coingecko_id,
        fromDay: window.fromDay,
        toDay: window.toDay,
      });
      const rows = buildDailyPriceRows({
        network: token.network,
        tokenAddress: token.token_address,
        coingeckoId: token.coingecko_id,
        prices: rangeResponse.prices || [],
        currentDay,
      }).filter((row) => row.day >= window.fromDay && row.day <= window.toDay);

      upsertedRows += await upsertDailyPrices(rows);
      await sleep(requestDelayMs);
    }
  }

  logLine("completed price refresh", {
    tokens: tokens.length,
    upserted_rows: upsertedRows,
    duration_ms: Math.round(performance.now() - startedAt),
  });
};

if (isDirectRun(import.meta.url)) {
  refreshPrices().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
