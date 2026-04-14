Vendored address-label inputs for DXTX serving metadata.

Files in this directory were copied from the local source projects used during development so production does not depend on sibling repositories being present.

Current provenance:
- `manual_labels.csv`, `cex_labels.csv`, `EIGEN_etherscan_labels_031625.csv`, `names.tab`: sourced from `eigen-index/assets/address-metadata/`
- `internet_manual_labels.csv`: curated DXTX additions from public explorer pages and other non-first-party web sources
- `first_party_exchange_labels.csv`: vendored on April 14, 2026 from official exchange reserve/address disclosures
  - `https://static.okx.com/cdn/okx/por/chain/por_csv_2026030319_V3.zip`
  - `https://static.bycustody.com/download/app/bybit_por_202212.csv`
  - `https://raw.githubusercontent.com/bitfinexcom/pub/main/wallets.txt`
  - `https://www.htx.com/support/en-us/detail/24922606430831`
  - `https://crypto.com/us/company-news/transparency-first`
  - `https://www.bybit.com/common-static/cht-static/por/Bybit_PoR_Audit_2025_Jul_24.pdf`
  - `https://support.deribit.com/hc/en-us/articles/31211285163165-Proof-Of-Reserves`
- `superset_cr_router_labels.sql`: sourced from `superset-cr/sql/ddl/000_init.sql`
- `eth_labels_accounts.csv`: exported on April 14, 2026 from `dawsbot/eth-labels` SQLite `accounts` table for chain ids `1`, `10`, `56`, `8453`, and `42161`

Operational note:
- `src/sync-labels.ts` reads only from this directory.
- Dune sources remain external by design and are loaded through the Dune API when a `config/dune-address-label-sources.json` file or `ADDRESS_LABELS_DUNE_CONFIG` path is provided.
- The uploaded-address Dune matching pass is optional and requires `DUNE_UPLOAD_NAMESPACE` or `--dune-upload-namespace=...`.
