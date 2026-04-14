import { createServingClient } from "./serving.js";

const normalizedNetworkSql = (column: string) => `
  case ${column}
    when 'ethereum' then 'eth'
    when 'base' then 'base'
    when 'arbitrum' then 'arbitrum'
    when 'optimism' then 'optimism'
    when 'polygon' then 'polygon'
    when 'avalanche' then 'avalanche'
    when 'bsc' then 'bsc'
    when 'unichain' then 'unichain'
    else ${column}
  end
`;

const createServingSchemaSql = `
  do $$
  begin
    begin
      alter table token_flow_daily_totals rename column chain to network;
    exception
      when undefined_column or duplicate_column or undefined_table then null;
    end;

    begin
      alter table token_flow_leaderboards rename column chain to network;
    exception
      when undefined_column or duplicate_column or undefined_table then null;
    end;

    begin
      alter table token_daily_address_flows rename column chain to network;
    exception
      when undefined_column or duplicate_column or undefined_table then null;
    end;

    if to_regclass('public.token_flow_daily_address_flows') is not null
       and to_regclass('public.token_daily_address_flows') is null then
      alter table token_flow_daily_address_flows rename to token_daily_address_flows;
    end if;
  end
  $$;

  create table if not exists token_flow_daily_totals (
    network text not null,
    token_address text not null,
    day date not null,
    transfer_count bigint not null,
    amount_native_sum double precision not null,
    avg_amount_native double precision,
    is_partial_day boolean not null,
    as_of_ts timestamptz not null,
    primary key (network, token_address, day)
  );

  create table if not exists token_daily_address_flows (
    network text not null,
    token_address text not null,
    day date not null,
    token_name text,
    token_symbol text,
    token_decimals integer,
    target_source text,
    coingecko_id text,
    coingecko_name text,
    coingecko_symbol text,
    address text not null,
    sent_transfer_count bigint not null,
    received_transfer_count bigint not null,
    total_transfer_count bigint not null,
    sent_amount_native_sum double precision not null,
    received_amount_native_sum double precision not null,
    gross_amount_native_sum double precision not null,
    net_amount_native_sum double precision not null,
    is_partial_day boolean not null,
    as_of_ts timestamptz not null,
    primary key (network, token_address, day, address)
  );

  create index if not exists idx_token_daily_address_flows_lookup
    on token_daily_address_flows (network, token_address, address, day);

  create index if not exists idx_token_daily_address_flows_coingecko_day
    on token_daily_address_flows (coingecko_id, day desc);

  create table if not exists token_flow_leaderboards (
    network text not null,
    token_address text not null,
    token_name text,
    token_symbol text,
    token_decimals integer,
    target_source text,
    coingecko_id text,
    coingecko_name text,
    coingecko_symbol text,
    window_days integer not null,
    window_start_day date not null,
    window_end_day date not null,
    is_partial_day boolean not null,
    metric text not null,
    address text not null,
    flow_rank bigint not null,
    transfer_count bigint not null,
    amount_native_sum double precision not null,
    avg_amount_native double precision,
    as_of_ts timestamptz not null,
    primary key (network, token_address, window_days, metric, flow_rank)
  );

  create table if not exists token_price_daily (
    network text not null,
    token_address text not null,
    coingecko_id text not null,
    day date not null,
    median_price_usd double precision not null,
    sample_count integer not null,
    source text not null,
    is_intraday boolean not null default false,
    first_price_ts timestamptz,
    last_price_ts timestamptz,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (network, token_address, day)
  );

  create index if not exists idx_token_price_daily_coingecko_day
    on token_price_daily (coingecko_id, day desc);

  create table if not exists address_labels_raw (
    network text not null,
    address text not null,
    source_name text not null,
    source_key text not null,
    source_type text not null,
    source_uri text,
    label text,
    category text,
    entity_type text,
    aggregator_label text,
    aggregator_source text,
    upstream_aggregator_label text,
    upstream_aggregator_source text,
    confidence double precision,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    source_recorded_at timestamptz,
    external_added_at timestamptz,
    primary key (network, address, source_name, source_key)
  );

  create index if not exists idx_address_labels_raw_network_address
    on address_labels_raw (network, address);

  create index if not exists idx_address_labels_raw_source_name
    on address_labels_raw (source_name, source_recorded_at desc);

  create table if not exists address_labels (
    network text not null,
    address text not null,
    label text not null,
    category text,
    entity_type text,
    aggregator_label text,
    aggregator_source text,
    upstream_aggregator_label text,
    upstream_aggregator_source text,
    source_name text not null,
    source_key text not null,
    source_rank integer not null,
    confidence double precision,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    source_recorded_at timestamptz,
    external_added_at timestamptz,
    primary key (network, address)
  );

  create index if not exists idx_address_labels_label
    on address_labels (label);

  create table if not exists token_flow_leaderboards_enriched (
    network text not null,
    token_address text not null,
    token_name text,
    token_symbol text,
    token_decimals integer,
    target_source text,
    coingecko_id text,
    coingecko_name text,
    coingecko_symbol text,
    window_days integer not null,
    window_start_day date not null,
    window_end_day date not null,
    is_partial_day boolean not null,
    metric text not null,
    address text not null,
    flow_rank bigint not null,
    transfer_count bigint not null,
    amount_native_sum double precision not null,
    amount_usd_sum double precision,
    avg_amount_native double precision,
    avg_amount_usd double precision,
    label text,
    category text,
    entity_type text,
    aggregator_label text,
    aggregator_source text,
    upstream_aggregator_label text,
    upstream_aggregator_source text,
    label_source_name text,
    label_source_key text,
    label_confidence double precision,
    label_source_recorded_at timestamptz,
    label_external_added_at timestamptz,
    as_of_ts timestamptz not null,
    primary key (network, token_address, window_days, metric, flow_rank)
  );

  create index if not exists idx_token_flow_leaderboards_enriched_lookup
    on token_flow_leaderboards_enriched (network, token_address, window_days, metric);

  create index if not exists idx_token_flow_leaderboards_enriched_address
    on token_flow_leaderboards_enriched (network, address);

  update token_flow_daily_totals
  set network = ${normalizedNetworkSql("network")}
  where network <> ${normalizedNetworkSql("network")};

  update token_flow_leaderboards
  set network = ${normalizedNetworkSql("network")}
  where network <> ${normalizedNetworkSql("network")};

  update token_daily_address_flows
  set network = ${normalizedNetworkSql("network")}
  where network <> ${normalizedNetworkSql("network")};
`;

const servingSchemaLockKey = 20_260_410;

export const ensureServingSchema = async () => {
  const client = createServingClient();
  await client.connect();

  try {
    await client.query("select pg_advisory_lock($1)", [servingSchemaLockKey]);
    await client.query(createServingSchemaSql);
  } finally {
    await client.query("select pg_advisory_unlock($1)", [servingSchemaLockKey]).catch(() => undefined);
    await client.end();
  }
};
