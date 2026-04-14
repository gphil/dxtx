Vendored address-label inputs for DXTX serving metadata.

Files in this directory were copied from the local source projects used during development so production does not depend on sibling repositories being present.

Current provenance:
- `manual_labels.csv`, `cex_labels.csv`, `EIGEN_etherscan_labels_031625.csv`, `names.tab`: sourced from `eigen-index/assets/address-metadata/`
- `superset_cr_router_labels.sql`: sourced from `superset-cr/sql/ddl/000_init.sql`

Operational note:
- `src/sync-labels.ts` reads only from this directory.
- Dune sources remain external by design and are loaded through the Dune API when a `config/dune-address-label-sources.json` file or `ADDRESS_LABELS_DUNE_CONFIG` path is provided.
