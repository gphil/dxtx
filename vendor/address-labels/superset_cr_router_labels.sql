CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analysis;

CREATE TABLE IF NOT EXISTS core.chain (
  id SMALLINT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.pool (
  chain_id SMALLINT NOT NULL REFERENCES core.chain(id),
  dex TEXT NOT NULL,
  address TEXT NOT NULL,
  token0_address TEXT NOT NULL,
  token1_address TEXT NOT NULL,
  fee_tier INTEGER,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (chain_id, dex, address)
);

CREATE TABLE IF NOT EXISTS core.swap (
  chain_id SMALLINT NOT NULL,
  dex TEXT NOT NULL,
  pool_address TEXT NOT NULL,
  tx_hash TEXT NOT NULL,
  evt_index INTEGER NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  sender TEXT,
  recipient TEXT,
  origin TEXT,
  amount0 NUMERIC NOT NULL,
  amount1 NUMERIC NOT NULL,
  amount_usd NUMERIC,
  fee INTEGER,
  tick INTEGER,
  liquidity NUMERIC,
  liquidity_token0 NUMERIC,
  liquidity_token1 NUMERIC,
  one_percent_depth0 NUMERIC,
  one_percent_depth1 NUMERIC,
  PRIMARY KEY (chain_id, dex, pool_address, tx_hash, evt_index),
  FOREIGN KEY (chain_id, dex, pool_address) REFERENCES core.pool(chain_id, dex, address)
);

ALTER TABLE core.swap
  ADD COLUMN IF NOT EXISTS sender TEXT,
  ADD COLUMN IF NOT EXISTS recipient TEXT,
  ADD COLUMN IF NOT EXISTS origin TEXT,
  ADD COLUMN IF NOT EXISTS liquidity_token0 NUMERIC,
  ADD COLUMN IF NOT EXISTS liquidity_token1 NUMERIC,
  ADD COLUMN IF NOT EXISTS one_percent_depth0 NUMERIC,
  ADD COLUMN IF NOT EXISTS one_percent_depth1 NUMERIC,
  DROP COLUMN IF EXISTS sqrt_price_x96,
  DROP COLUMN IF EXISTS trader,
  DROP COLUMN IF EXISTS router;

CREATE TABLE IF NOT EXISTS analysis.router_labels (
  chain TEXT NOT NULL,
  address TEXT NOT NULL,
  label TEXT NOT NULL,
  category TEXT NOT NULL,
  entity_type TEXT,
  aggregator_label TEXT,
  aggregator_source TEXT,
  upstream_aggregator_label TEXT,
  upstream_aggregator_source TEXT,
  source TEXT,
  notes TEXT,
  PRIMARY KEY (chain, address)
);

ALTER TABLE analysis.router_labels
  ADD COLUMN IF NOT EXISTS entity_type TEXT,
  ADD COLUMN IF NOT EXISTS aggregator_label TEXT,
  ADD COLUMN IF NOT EXISTS aggregator_source TEXT,
  ADD COLUMN IF NOT EXISTS upstream_aggregator_label TEXT,
  ADD COLUMN IF NOT EXISTS upstream_aggregator_source TEXT;

INSERT INTO analysis.router_labels (chain, address, label, category, entity_type, source, notes)
VALUES
  (
    'ethereum',
    '0x66a9893cc07d91d95644aedd05d03f95e1dba8af',
    'Uniswap V4 Universal Router',
    'router',
    'contract',
    'etherscan',
    'Public name tag on Etherscan.'
  ),
  (
    'ethereum',
    '0xe592427a0aece92de3edee1f18e0157c05861564',
    'Uniswap V3 Router',
    'router',
    'contract',
    'etherscan',
    'Public name tag on Etherscan.'
  ),
  (
    'ethereum',
    '0x63242a4ea82847b20e506b63b0e2e2eff0cc6cb0',
    'KyberSwap Aggregator Executor V3',
    'router',
    'contract',
    'etherscan',
    'Public name tag on Etherscan.'
  ),
  (
    'ethereum',
    '0x5050e08626c499411b5d0e0b5af0e83d3fd82edf',
    'MEV Bot 0x50...EDF',
    'mev',
    'contract',
    'etherscan',
    'Explorer tooltip label.'
  ),
  (
    'ethereum',
    '0x6747bcaf9bd5a5f0758cbe08903490e45ddfacb5',
    'UniversalUniswapV3Adaptor',
    'aggregator',
    'contract',
    'etherscan',
    'Verified contract name on Etherscan.'
  ),
  (
    'ethereum',
    '0xf48a3f7c0575c85cf4529aa220caf3c055773f1c',
    'MainnetSettler',
    'aggregator',
    'contract',
    'etherscan',
    'Verified contract name on Etherscan; likely the 0x execution layer based on nearby labeled flows.'
  ),
  (
    'ethereum',
    '0x51c72848c68a965f66fa7a88855f9f7784502a7f',
    'MEV Bot (Runtime-Constraint Pattern)',
    'mev',
    'contract',
    'research_paper',
    'Identified in public MEV-bot research as matching a runtime-constraint transaction pattern.'
  ),
  (
    'ethereum',
    '0xfbd4cdb413e45a52e2c8312f670e9ce67e794c37',
    'Searcher / Firm Contract 0xfbd4...4c37',
    'searcher',
    'contract',
    'flow_shape',
    'High-volume, low-origin contract flow with no public name tag; treated as searcher or trading-firm execution.'
  ),
  (
    'ethereum',
    '0xbdb3ba9ffe392549e1f8658dd2630c141fdf47b6',
    'Unlabeled Ethereum Sender 0xbdb3...47b6',
    'unknown',
    'contract',
    'etherscan',
    'High-volume contract; no public name tag found.'
  ),
  (
    'ethereum',
    '0x1f2f10d1c40777ae1da742455c65828ff36df387',
    'Unlabeled Ethereum Sender 0x1f2f...f387',
    'unknown',
    'contract',
    'etherscan',
    'Single-origin high-volume sender; no public name tag found.'
  ),
  (
    'ethereum',
    '0x365084b05fa7d5028346bd21d842ed0601bab5b8',
    'Odos Executor',
    'aggregator',
    'contract',
    'family_promotion',
    'Matched repeated Odos V2/V3 counterparties and direct Odos Router V3 transaction traces.'
  ),
  (
    'ethereum',
    '0x990636ecb3ff04d33d92e970d3d588bf5cd8d086',
    '1inch Aggregation Executor 5',
    'aggregator',
    'contract',
    'family_promotion',
    'Matched explorer public name tag "1inch: Aggregation Executor 5".'
  ),
  (
    'ethereum',
    '0x111111125421ca6dc452d289314280a0f8842a65',
    '1inch Aggregation Router V5',
    'aggregator',
    'contract',
    'etherscan',
    'Known 1inch aggregation router deployment.'
  ),
  (
    'ethereum',
    '0x5745050e787f693ed21e4418d528f78ad9c374a6',
    'UniV4Adapter',
    'aggregator',
    'contract',
    'explorer_contract',
    'Verified contract name from explorer/source-family matches.'
  ),
  (
    'ethereum',
    '0x163f3103de041d25464e2c8a4f8f3187ec1856e0',
    'UniV4HookAdapter',
    'aggregator',
    'contract',
    'explorer_contract',
    'Verified contract name from explorer/source-family matches.'
  ),
  (
    'ethereum',
    '0x1f560aee3d0c615f51466dcd9a312cfa858271a5',
    '0x Settler Family',
    'aggregator',
    'contract',
    'family_promotion',
    'Matched explorer counterparty label "0x: Allowance Holder".'
  ),
  (
    'ethereum',
    '0x5a1b2acd0c9992e14f2b6d8a326023e35a097a3a',
    '0x Settler Family',
    'aggregator',
    'contract',
    'family_promotion',
    'Matched explorer counterparty label "0x: Allowance Holder".'
  ),
  (
    'ethereum',
    '0x2b3acfc9f10d52cd3a5bfc2639470e3a7ed15070',
    'Binance DEX Executor Cluster',
    'aggregator',
    'contract',
    'manual_family_inference',
    'Repeatedly co-occurs with 0x3be1... and routes through Binance: DEX Router in shared transactions.'
  ),
  (
    'ethereum',
    '0x3be1f5f596f21fb7a1a0cb5e61c9845e31f395b8',
    'Binance DEX Executor Cluster',
    'aggregator',
    'contract',
    'manual_family_inference',
    'Repeatedly co-occurs with 0x2b3a... and routes through Binance: DEX Router in shared transactions.'
  ),
  (
    'base',
    '0x6ff5693b99212da76ad316178a184ab56d299b43',
    'Uniswap V4 Universal Router',
    'router',
    'contract',
    'basescan',
    'Public name tag on BaseScan.'
  ),
  (
    'base',
    '0x83d55acdc72027ed339d267eebaf9a41e47490d5',
    'Searcher / Firm Contract 0x83d5...90d5',
    'searcher',
    'contract',
    'flow_shape',
    'High-volume, low-origin contract flow with no public name tag; treated as searcher or trading-firm execution.'
  ),
  (
    'base',
    '0x3725bd4d175283108156c3f15f86e1c51266155d',
    'Searcher / Firm Proxy 0x3725...155d',
    'searcher',
    'contract',
    'trace_inference',
    'Most scoped flow routes to a single proxy recipient, consistent with a firm or searcher execution cluster.'
  ),
  (
    'base',
    '0xbf44de8fc9eeeed8615b0b3bc095cb0ddef35e09',
    'Cross-Venue Executor (Odos + Pancake)',
    'aggregator',
    'contract',
    'family_promotion',
    'Matched repeated Odos V2/V3 and Pancake Infinity Vault counterparties.'
  ),
  (
    'base',
    '0x63242a4ea82847b20e506b63b0e2e2eff0cc6cb0',
    'KyberSwap Aggregator Executor V3',
    'router',
    'contract',
    'address-reuse',
    'Same contract address and label as the verified Ethereum deployment.'
  ),
  (
    'base',
    '0x3a32d2987b86a9c5552921289b8d4470075d360f',
    'Unverified Base Contract 0x3a32...360f',
    'unknown',
    'contract',
    'rpc',
    'Contract bytecode detected via eth_getCode; no public name tag found.'
  ),
  (
    'base',
    '0x88fec94eb9f11376e0dcb31ea7babc278d6035c3',
    'Unverified Base Contract 0x88fe...35c3',
    'unknown',
    'contract',
    'rpc',
    'Contract bytecode detected via eth_getCode; no public name tag found.'
  ),
  (
    'base',
    '0x4d838e8b8b7737ecbfd9e0d5288647db22d2bb81',
    'Unverified Base Contract 0x4d83...bb81',
    'unknown',
    'contract',
    'rpc',
    'Contract bytecode detected via eth_getCode; no public name tag found.'
  ),
  (
    'base',
    '0x43f9a7aec2a683c4cd6016f92ff76d5f3e7b44d3',
    'Magpie Executor',
    'aggregator',
    'contract',
    'trace_inference',
    'BaseScan traces show MagpieRouterV3_1 forwarding swap flow into this executor on Base.'
  ),
  (
    'base',
    '0x42ef85bc817b2c5920d60575b1b64984164b849b',
    'IceCreamSwap Executor',
    'aggregator',
    'contract',
    'trace_inference',
    'BaseScan traces show IcecreamSwap v2 AggregatorGuard and LI.FI Diamond using this address as the executor.'
  ),
  (
    'base',
    '0x8592f547a55f3edb54cd38c90333e310207d7938',
    'Arba Executor',
    'aggregator',
    'contract',
    'trace_inference',
    'BaseScan traces show repeated direct calls to this executor and revert strings prefixed with "Arba:".'
  ),
  (
    'base',
    '0xc4a53222a5905212fa08740d7cb6c3264962629d',
    'Binance DEX Executor',
    'aggregator',
    'contract',
    'trace_inference',
    'BaseScan traces show Binance: DEX Router transferring assets into this executor during swap flow.'
  ),
  (
    'base',
    '0x990636ecb3ff04d33d92e970d3d588bf5cd8d086',
    '1inch Aggregation Executor 5',
    'aggregator',
    'contract',
    'family_promotion',
    'Matched same-address aggregator label on another chain plus repeated 1inch router counterparties.'
  ),
  (
    'base',
    '0x785648669b8e90a75a6a8de682258957f9028462',
    '0x Settler Family',
    'aggregator',
    'contract',
    'family_promotion',
    'Matched Settler-family verified contract naming.'
  ),
  (
    'base',
    '0xdc5d8200a030798bc6227240f68b4dd9542686ef',
    '0x Settler Family',
    'aggregator',
    'contract',
    'family_promotion',
    'Matched explorer counterparty label "0x: Allowance Holder".'
  ),
  (
    'base',
    '0x49fb9c16b9b2a19452633573603c837673fd7e04',
    '0x Settler Family',
    'aggregator',
    'contract',
    'family_promotion',
    'Matched explorer counterparty label "0x: Allowance Holder".'
  ),
  (
    'base',
    '0x49e3d83b6360dcbd4861f73d2eb3feb06e29fd2e',
    'UniV4Adapter',
    'aggregator',
    'contract',
    'sourcify',
    'Verified contract name from Sourcify metadata.'
  ),
  (
    'base',
    '0x411d2c093e4c2e69bf0d8e94be1bf13dadd879c6',
    'UniversalUniswapV3Adaptor',
    'aggregator',
    'contract',
    'cross_chain_explorer',
    'Same adapter contract name appears on verified cross-chain deployments.'
  ),
  (
    'base',
    '0x2626664c2603336e57b271c5c0b26f421741e481',
    'Uniswap V3 Swap Router02',
    'router',
    'contract',
    'basescan',
    'Public name tag on BaseScan.'
  ),
  (
    'sei',
    '0x11da6463d6cb5a03411dbf5ab6f6bc3997ac7428',
    'DragonSwap Swap Router 02',
    'router',
    'contract',
    'seiscan',
    'SeiScan transaction pages identify this address as DragonSwap: Swap Router 02.'
  ),
  (
    'sei',
    '0xcacbf14f07af3be93c6fc8bf0a43f9279f792199',
    'DragonSwap Split Router',
    'router',
    'contract',
    'sourcify',
    'Verified contract name from Sourcify metadata.'
  ),
  (
    'sei',
    '0x04954f93d189c9afb6e09c47a55d9a124537ab08',
    'Unverified Contract',
    'unknown',
    'contract',
    'rpc',
    'Contract bytecode detected via eth_getCode; no verified name resolved.'
  ),
  (
    'unichain',
    '0xef740bf23acae26f6492b10de645d6b98dc8eaf3',
    'Uniswap Universal Router',
    'router',
    'contract',
    'sourcify',
    'Verified contract name from Sourcify metadata and Unichain documentation.'
  ),
  (
    'unichain',
    '0x2ad99160cc5e3230498a02ef1394987c4d3ec9db',
    'Unverified Contract',
    'unknown',
    'contract',
    'rpc',
    'Contract bytecode detected via eth_getCode; no verified name resolved.'
  ),
  (
    'unichain',
    '0x8a952f7e246439773114da6b9fce3d7cc5cd0de0',
    'Unverified Contract',
    'unknown',
    'contract',
    'rpc',
    'Contract bytecode detected via eth_getCode; no verified name resolved.'
  ),
  (
    'ethereum',
    '0xbc1d9760bd6ca468ca9fb5ff2cfbeac35d86c973',
    'Systematic Trading Contract 0xbc1d...',
    'searcher',
    'contract',
    'other_bucket_top1000',
    'Top-1000 Other-bucket contract by swaps per origin; promoted into systematic trading bucket for diagnostics.'
  ),
  (
    'ethereum',
    '0x3611b82c7b13e72b26eb0e9be0613bee7a45ac7c',
    'Systematic Trading Contract 0x3611...',
    'searcher',
    'contract',
    'other_bucket_top1000',
    'Top-1000 Other-bucket contract by swaps per origin; promoted into systematic trading bucket for diagnostics.'
  )
ON CONFLICT (chain, address) DO UPDATE
SET
  label = EXCLUDED.label,
  category = EXCLUDED.category,
  entity_type = EXCLUDED.entity_type,
  source = EXCLUDED.source,
  notes = EXCLUDED.notes;

INSERT INTO analysis.router_labels (chain, address, label, category, entity_type, source, notes)
VALUES
  (
    'base',
    '0x0bb7a4fdea32910038dec59c20ccae3a6e66b09f',
    '1inch Swap Executor',
    'aggregator',
    'contract',
    'trace_inference',
    'Swap traces show 1inch swap executor style calldata and repeated routing into the 1inch router.'
  ),
  (
    'ethereum',
    '0xa5f91e598668040055dc861a7316e677a5b730b0',
    'LiquidMesh Executor',
    'aggregator',
    'contract',
    'trace_inference',
    'Recipient flow repeatedly routes into the documented LiquidMesh router address 0x3d90... on Ethereum.'
  ),
  (
    'sei',
    '0xfde9ce4e17b650efdca13d524f132876700d806f',
    'LiFiDEXAggregator',
    'aggregator',
    'contract',
    'explorer_contract',
    'Verified contract name resolved for this Sei address.'
  ),
  (
    'ethereum',
    '0x0bb7a4fdea32910038dec59c20ccae3a6e66b09f',
    '1inch Swap Executor',
    'aggregator',
    'contract',
    'address_reuse',
    'Same contract address and execution pattern as the Base 1inch swap executor.'
  ),
  (
    'ethereum',
    '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',
    'Uniswap V3 Swap Router02',
    'router',
    'contract',
    'address_reuse',
    'Known Uniswap SwapRouter02 deployment on Ethereum.'
  )
ON CONFLICT (chain, address) DO UPDATE
SET
  label = EXCLUDED.label,
  category = EXCLUDED.category,
  entity_type = EXCLUDED.entity_type,
  source = EXCLUDED.source,
  notes = EXCLUDED.notes;

UPDATE analysis.router_labels labels
SET
  aggregator_label = aggregator.label,
  aggregator_source = aggregator.source
FROM (
  VALUES
    ('ethereum', '0x66a9893cc07d91d95644aedd05d03f95e1dba8af', 'Uniswap', 'etherscan'),
    ('ethereum', '0xe592427a0aece92de3edee1f18e0157c05861564', 'Uniswap', 'etherscan'),
    ('ethereum', '0x63242a4ea82847b20e506b63b0e2e2eff0cc6cb0', 'KyberSwap', 'etherscan'),
    ('ethereum', '0x6747bcaf9bd5a5f0758cbe08903490e45ddfacb5', 'OKX DEX', 'family_inference'),
    ('ethereum', '0xf48a3f7c0575c85cf4529aa220caf3c055773f1c', '0x / Matcha', 'family_inference'),
    ('ethereum', '0x365084b05fa7d5028346bd21d842ed0601bab5b8', 'Odos', 'trace_inference'),
    ('ethereum', '0x990636ecb3ff04d33d92e970d3d588bf5cd8d086', '1inch', 'etherscan'),
    ('ethereum', '0x111111125421ca6dc452d289314280a0f8842a65', '1inch', 'etherscan'),
    ('ethereum', '0x0bb7a4fdea32910038dec59c20ccae3a6e66b09f', '1inch', 'address_reuse'),
    ('ethereum', '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45', 'Uniswap', 'address_reuse'),
    ('ethereum', '0x5745050e787f693ed21e4418d528f78ad9c374a6', 'OKX DEX', 'family_inference'),
    ('ethereum', '0x163f3103de041d25464e2c8a4f8f3187ec1856e0', 'OKX DEX', 'family_inference'),
    ('ethereum', '0x1f560aee3d0c615f51466dcd9a312cfa858271a5', '0x / Matcha', 'family_inference'),
    ('ethereum', '0x5a1b2acd0c9992e14f2b6d8a326023e35a097a3a', '0x / Matcha', 'family_inference'),
    ('ethereum', '0x2b3acfc9f10d52cd3a5bfc2639470e3a7ed15070', 'Binance DEX', 'trace_inference'),
    ('ethereum', '0x3be1f5f596f21fb7a1a0cb5e61c9845e31f395b8', 'Binance DEX', 'trace_inference'),
    ('ethereum', '0xa5f91e598668040055dc861a7316e677a5b730b0', 'LiquidMesh', 'trace_inference'),
    ('ethereum', '0x51c72848c68a965f66fa7a88855f9f7784502a7f', 'Known MEV Bot Contract', 'research_paper'),
    ('ethereum', '0xfbd4cdb413e45a52e2c8312f670e9ce67e794c37', 'Systematic Trading Contract', 'flow_shape'),
    ('ethereum', '0xbc1d9760bd6ca468ca9fb5ff2cfbeac35d86c973', 'Systematic Trading Contract', 'other_bucket_top1000'),
    ('ethereum', '0x3611b82c7b13e72b26eb0e9be0613bee7a45ac7c', 'Systematic Trading Contract', 'other_bucket_top1000'),
    ('base', '0x6ff5693b99212da76ad316178a184ab56d299b43', 'Uniswap', 'basescan'),
    ('base', '0x0bb7a4fdea32910038dec59c20ccae3a6e66b09f', '1inch', 'trace_inference'),
    ('base', '0xbf44de8fc9eeeed8615b0b3bc095cb0ddef35e09', 'Odos', 'trace_inference'),
    ('base', '0x63242a4ea82847b20e506b63b0e2e2eff0cc6cb0', 'KyberSwap', 'address_reuse'),
    ('base', '0x43f9a7aec2a683c4cd6016f92ff76d5f3e7b44d3', 'Magpie', 'trace_inference'),
    ('base', '0x42ef85bc817b2c5920d60575b1b64984164b849b', 'LI.FI', 'trace_inference'),
    ('base', '0x8592f547a55f3edb54cd38c90333e310207d7938', 'Arba', 'trace_inference'),
    ('base', '0xc4a53222a5905212fa08740d7cb6c3264962629d', 'Binance DEX', 'trace_inference'),
    ('base', '0x83d55acdc72027ed339d267eebaf9a41e47490d5', 'Systematic Trading Contract', 'flow_shape'),
    ('base', '0x3725bd4d175283108156c3f15f86e1c51266155d', 'Systematic Trading Contract', 'trace_inference'),
    ('base', '0x990636ecb3ff04d33d92e970d3d588bf5cd8d086', '1inch', 'family_inference'),
    ('base', '0x785648669b8e90a75a6a8de682258957f9028462', '0x / Matcha', 'family_inference'),
    ('base', '0xdc5d8200a030798bc6227240f68b4dd9542686ef', '0x / Matcha', 'family_inference'),
    ('base', '0x49fb9c16b9b2a19452633573603c837673fd7e04', '0x / Matcha', 'family_inference'),
    ('base', '0x49e3d83b6360dcbd4861f73d2eb3feb06e29fd2e', 'OKX DEX', 'family_inference'),
    ('base', '0x411d2c093e4c2e69bf0d8e94be1bf13dadd879c6', 'OKX DEX', 'family_inference'),
    ('base', '0x2626664c2603336e57b271c5c0b26f421741e481', 'Uniswap', 'basescan'),
    ('sei', '0x11da6463d6cb5a03411dbf5ab6f6bc3997ac7428', 'DragonSwap', 'seiscan'),
    ('sei', '0xcacbf14f07af3be93c6fc8bf0a43f9279f792199', 'DragonSwap', 'sourcify'),
    ('sei', '0xfde9ce4e17b650efdca13d524f132876700d806f', 'LI.FI', 'explorer_contract'),
    ('unichain', '0xef740bf23acae26f6492b10de645d6b98dc8eaf3', 'Uniswap', 'sourcify')
) AS aggregator(chain, address, label, source)
WHERE labels.chain = aggregator.chain
  AND labels.address = aggregator.address;

UPDATE analysis.router_labels labels
SET
  upstream_aggregator_label = upstream.label,
  upstream_aggregator_source = upstream.source
FROM (
  VALUES
    ('ethereum', '0x66a9893cc07d91d95644aedd05d03f95e1dba8af', 'Uniswap', 'etherscan'),
    ('ethereum', '0xe592427a0aece92de3edee1f18e0157c05861564', 'Uniswap', 'etherscan'),
    ('ethereum', '0x63242a4ea82847b20e506b63b0e2e2eff0cc6cb0', 'KyberSwap', 'etherscan'),
    ('ethereum', '0x6747bcaf9bd5a5f0758cbe08903490e45ddfacb5', 'OKX DEX', 'family_inference'),
    ('ethereum', '0xf48a3f7c0575c85cf4529aa220caf3c055773f1c', '0x / Matcha', 'family_inference'),
    ('ethereum', '0x365084b05fa7d5028346bd21d842ed0601bab5b8', 'Odos', 'trace_inference'),
    ('ethereum', '0x990636ecb3ff04d33d92e970d3d588bf5cd8d086', '1inch', 'etherscan'),
    ('ethereum', '0x5745050e787f693ed21e4418d528f78ad9c374a6', 'OKX DEX', 'family_inference'),
    ('ethereum', '0x163f3103de041d25464e2c8a4f8f3187ec1856e0', 'OKX DEX', 'family_inference'),
    ('ethereum', '0x1f560aee3d0c615f51466dcd9a312cfa858271a5', '0x / Matcha', 'family_inference'),
    ('ethereum', '0x5a1b2acd0c9992e14f2b6d8a326023e35a097a3a', '0x / Matcha', 'family_inference'),
    ('ethereum', '0x2b3acfc9f10d52cd3a5bfc2639470e3a7ed15070', 'Binance DEX', 'trace_inference'),
    ('ethereum', '0x3be1f5f596f21fb7a1a0cb5e61c9845e31f395b8', 'Binance DEX', 'trace_inference'),
    ('base', '0x6ff5693b99212da76ad316178a184ab56d299b43', 'Uniswap', 'basescan'),
    ('base', '0xbf44de8fc9eeeed8615b0b3bc095cb0ddef35e09', 'Odos / Pancake', 'trace_inference'),
    ('base', '0x63242a4ea82847b20e506b63b0e2e2eff0cc6cb0', 'KyberSwap', 'address_reuse'),
    ('base', '0x43f9a7aec2a683c4cd6016f92ff76d5f3e7b44d3', 'Magpie', 'trace_inference'),
    ('base', '0x42ef85bc817b2c5920d60575b1b64984164b849b', 'LI.FI / IceCreamSwap', 'trace_inference'),
    ('base', '0x8592f547a55f3edb54cd38c90333e310207d7938', 'Arba', 'trace_inference'),
    ('base', '0xc4a53222a5905212fa08740d7cb6c3264962629d', 'Binance DEX', 'trace_inference'),
    ('base', '0x990636ecb3ff04d33d92e970d3d588bf5cd8d086', '1inch', 'family_inference'),
    ('base', '0x785648669b8e90a75a6a8de682258957f9028462', '0x / Matcha', 'family_inference'),
    ('base', '0xdc5d8200a030798bc6227240f68b4dd9542686ef', '0x / Matcha', 'family_inference'),
    ('base', '0x49fb9c16b9b2a19452633573603c837673fd7e04', '0x / Matcha', 'family_inference'),
    ('base', '0x49e3d83b6360dcbd4861f73d2eb3feb06e29fd2e', 'OKX DEX', 'family_inference'),
    ('base', '0x411d2c093e4c2e69bf0d8e94be1bf13dadd879c6', 'OKX DEX', 'family_inference'),
    ('base', '0x2626664c2603336e57b271c5c0b26f421741e481', 'Uniswap', 'basescan'),
    ('sei', '0x11da6463d6cb5a03411dbf5ab6f6bc3997ac7428', 'DragonSwap', 'seiscan'),
    ('sei', '0xcacbf14f07af3be93c6fc8bf0a43f9279f792199', 'DragonSwap', 'sourcify'),
    ('unichain', '0xef740bf23acae26f6492b10de645d6b98dc8eaf3', 'Uniswap', 'sourcify')
) AS upstream(chain, address, label, source)
WHERE labels.chain = upstream.chain
  AND labels.address = upstream.address;

UPDATE analysis.router_labels
SET
  label = '1inch Swap Executor',
  category = 'aggregator',
  entity_type = 'contract',
  source = 'address_reuse',
  notes = 'Same contract address and execution pattern as the Base 1inch swap executor.',
  aggregator_label = '1inch',
  aggregator_source = 'address_reuse'
WHERE chain = 'ethereum'
  AND lower(address) = '0x0bb7a4fdea32910038dec59c20ccae3a6e66b09f';

UPDATE analysis.router_labels
SET
  label = 'Uniswap V3 Swap Router02',
  category = 'router',
  entity_type = 'contract',
  source = 'address_reuse',
  notes = 'Known Uniswap SwapRouter02 deployment on Ethereum.',
  aggregator_label = 'Uniswap',
  aggregator_source = 'address_reuse'
WHERE chain = 'ethereum'
  AND lower(address) = '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45';

INSERT INTO analysis.router_labels (
  chain,
  address,
  label,
  category,
  entity_type,
  aggregator_label,
  aggregator_source,
  source,
  notes
)
VALUES
  (
    'ethereum',
    '0x6a000f20005980200259b80c5102003040001068',
    'Velora Augustus V6',
    'aggregator',
    'contract',
    'Velora',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters fees/aave-v3.ts as VeloraAugustusV6.'
  ),
  (
    'base',
    '0x6a000f20005980200259b80c5102003040001068',
    'Velora Augustus V6',
    'aggregator',
    'contract',
    'Velora',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters fees/aave-v3.ts as VeloraAugustusV6.'
  ),
  (
    'ethereum',
    '0x6352a56caadc4f1e25cd6c75970fa768a3304e64',
    'OpenOcean Router',
    'aggregator',
    'contract',
    'OpenOcean',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/openocean/index.ts.'
  ),
  (
    'base',
    '0x6352a56caadc4f1e25cd6c75970fa768a3304e64',
    'OpenOcean Router',
    'aggregator',
    'contract',
    'OpenOcean',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/openocean/index.ts.'
  ),
  (
    'unichain',
    '0x6352a56caadc4f1e25cd6c75970fa768a3304e64',
    'OpenOcean Router',
    'aggregator',
    'contract',
    'OpenOcean',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/openocean/index.ts.'
  ),
  (
    'ethereum',
    '0xdef1c0ded9bec7f1a1670819833240f027b25eff',
    '0x Exchange Proxy',
    'aggregator',
    'contract',
    '0x / Matcha',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters and aligned with 0x direct router branding.'
  ),
  (
    'base',
    '0xdef1c0ded9bec7f1a1670819833240f027b25eff',
    '0x Exchange Proxy',
    'aggregator',
    'contract',
    '0x / Matcha',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters and aligned with 0x direct router branding.'
  ),
  (
    'ethereum',
    '0x9008d19f58aabd9ed0d60971565aa8510560ab41',
    'GPv2Settlement',
    'aggregator',
    'contract',
    'CoW Protocol',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/cowswap/index.ts.'
  ),
  (
    'base',
    '0x9008d19f58aabd9ed0d60971565aa8510560ab41',
    'GPv2Settlement',
    'aggregator',
    'contract',
    'CoW Protocol',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/cowswap/index.ts.'
  ),
  (
    'ethereum',
    '0x881d40237659c251811cec9c364ef91dc08d300c',
    'MetaMask Swaps Router',
    'aggregator',
    'contract',
    'MetaMask Swaps',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/metamask.ts.'
  ),
  (
    'ethereum',
    '0x1a1ec25dc08e98e5e93f1104b5e5cdd298707d31',
    'MetaMask Swaps Router',
    'aggregator',
    'contract',
    'MetaMask Swaps',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/metamask.ts.'
  ),
  (
    'ethereum',
    '0x9dda6ef3d919c9bc8885d5560999a3640431e8e6',
    'MetaMask Swaps Router',
    'aggregator',
    'contract',
    'MetaMask Swaps',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/metamask.ts.'
  ),
  (
    'base',
    '0x9dda6ef3d919c9bc8885d5560999a3640431e8e6',
    'MetaMask Swaps Router',
    'aggregator',
    'contract',
    'MetaMask Swaps',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/metamask.ts.'
  ),
  (
    'base',
    '0x962287c9d5b8a682389e61edae90ec882325d08b',
    'MetaMask Swaps Router',
    'aggregator',
    'contract',
    'MetaMask Swaps',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/metamask.ts.'
  ),
  (
    'base',
    '0xb165c4d4b8044d4a9276c3d75f08cd6a2874a3b2',
    'MetaMask Swaps Router',
    'aggregator',
    'contract',
    'MetaMask Swaps',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/metamask.ts.'
  ),
  (
    'ethereum',
    '0xb300000b72deaeb607a12d5f54773d1c19c7028d',
    'Binance Wallet DEX Router',
    'aggregator',
    'contract',
    'Binance DEX',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/binancewallet/index.ts.'
  ),
  (
    'base',
    '0xb300000b72deaeb607a12d5f54773d1c19c7028d',
    'Binance Wallet DEX Router',
    'aggregator',
    'contract',
    'Binance DEX',
    'defillama_dimension_adapters',
    'defillama_dimension_adapters',
    'Mined from DefiLlama dimension-adapters aggregators/binancewallet/index.ts.'
  )
ON CONFLICT (chain, address) DO UPDATE
SET
  label = EXCLUDED.label,
  category = EXCLUDED.category,
  entity_type = EXCLUDED.entity_type,
  aggregator_label = EXCLUDED.aggregator_label,
  aggregator_source = EXCLUDED.aggregator_source,
  source = EXCLUDED.source,
  notes = EXCLUDED.notes;

CREATE INDEX IF NOT EXISTS idx_swap_pool_ts
  ON core.swap (chain_id, dex, pool_address, ts);

CREATE INDEX IF NOT EXISTS idx_swap_ts
  ON core.swap (ts);

CREATE INDEX IF NOT EXISTS idx_router_labels_address
  ON analysis.router_labels (address);
