# Frozen dashboard

A **static, read-only** rebuild of the dashboard, published at
**https://data.spacecode.tech**.

## Why this exists

The live Flask dashboard (`../dashboard.py`) runs on the 10-VM Spark cluster and collects
data continuously. This is the opposite: data collection is **stopped**, there is already
enough parquet, and this serves a snapshot as plain HTML from nginx — no Spark, no Flask, no
data collection, near-zero RAM. Nothing to crash.

It also does **not** include the price-prediction page (forecasting the market was removed).
It keeps the recommender and portfolio views, presented as analysis of historical data with a
clear "not financial advice" notice.

## Build

`build.py` reads the parquet in `../data/crypto`, runs `investment_recommender.py` and
`portfolio_optimizer.py` once (forced onto the already-loaded DataFrame so they don't re-scan
16k files), and writes self-contained pages to `site/`.

## The methodology page

`metodologjia.html` is the thesis chapter: the pipeline, the data source, every column
(unit, meaning, why it is kept), why Parquet, the partitioning scheme, the small-file
problem, the analysis methods, and the limitations.

Its numbers are **measured on every build** by `measure_corpus()`, never hand-written — it
rewrites the corpus into CSV/JSON/Parquet variants in a temp dir to compare sizes, and walks
the data dir for file counts and real block allocation. Two things that fall out of that, and
which the page states plainly rather than hiding:

- **`csv.gz` and `parquet+gzip` are both SMALLER than the chosen `parquet+snappy`.** Size is
  therefore not the reason to pick Parquet; splittability, column projection and embedded
  types are. `csv.gz` is disqualified (unsplittable), `parquet+gzip` is a deliberate
  speed-over-size trade.
- **The corpus is its own worst-case example of the small-file problem**: 868 KB of data,
  written as 15,948 tiny files plus ~16k `.crc` sidecars, consumes ~131 MB of disk — a ~151×
  amplification caused purely by how it was written.

⚠️ Use `os.walk`, not `glob`, when counting those files — `glob` silently skips dotfiles, and
Spark's `.crc` sidecars all start with a dot (this undercounted them 16,162 → 37 once already).

```bash
# On Ampere (this workspace's box has no python):
ssh ampere 'sudo docker run --rm --user 1000:1000 \
  -v /mnt/data/workspace:/w -w /w/big-data-dashboard -e HOME=/tmp \
  python:3.12-slim sh -c "pip install -q pandas pyarrow numpy && python frozen/build.py"'
```

`site/` is git-ignored (it's build output). `chart.min.js` is vendored into
`site/assets/` at deploy time so nginx serves it locally — no CDN.

## Deploy

`site/` is copied to `~/apps/data-dashboard/site` on Ampere and served by the nginx compose
there on `127.0.0.1:8165`. The Cloudflare tunnel routes `data.spacecode.tech` to it
(ingress inserted **before** the `*.spacecode.tech` wildcard, which otherwise sends every
subdomain to the POS). To refresh the data: rebuild, re-copy `site/`, `docker compose restart`.
