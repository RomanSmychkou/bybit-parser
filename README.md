# Bybit parser

Small, dependency-free downloaders for Bybit public history:

- `download_public_trades.py` downloads spot trade `*.csv.gz` files.
- `download_orderbooks.py` downloads `ob500` and `ob200` `*.data.zip` files.

The default layout keeps files grouped by symbol. The
`--flat-output` option writes files directly into the selected output
directory, which is useful for consumers that scan one raw-history directory.
Existing files are skipped.

## Standalone usage

```bash
python download_public_trades.py BTCUSDT SOLUSDT \
  --output data_storage/raw_history/spot_pt \
  --flat-output \
  --start 2025-01-01 \
  --end 2025-01-31
```

## Research Framework integration

When this repository is checked out next to `research-framework`, run from the
framework root:

```bash
python research-framework/scripts/download_bybit_history.py BTCUSDT --source both
```

The framework launcher passes the compatible flat layout and writes to
`research-framework/data_storage/raw_history/{spot_pt,spot_ob}`. If the parser
is elsewhere, set `BYBIT_PARSER_ROOT` or pass `--parser-root`.
