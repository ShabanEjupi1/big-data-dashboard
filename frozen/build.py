"""
Ndërtues i panelit të NGRIRË — lexon parquet-et një herë dhe nxjerr faqe statike.

Pse statike, e jo Flask-u origjinal:
  - "I ngrirë" do të thotë pikërisht kjo: mbledhja e të dhënave ka ndaluar, ka mjaftueshëm
    parquet, dhe paneli s'ka nevojë të llogarisë asgjë në kohë reale. Një grup skedarësh
    HTML të shërbyer nga nginx nuk zë RAM dhe s'ka çka të rrëzohet — dhe kutia Ampere tashmë
    ka rënë një herë nga mungesa e RAM-it.
  - Shabllonet origjinale (templates/*.html) rrojnë vetëm në VM-t, të cilat nuk arrihen nga
    kjo makinë. Kjo i rindërton nga e para, e vetëmjaftueshme.

Çka HIQET, me qëllim:
  - Faqja e PARASHIKIMEVE (ml_predictions). Parashikimi i së ardhmes u hoq me kërkesë —
    ky ndërtues as nuk e importon modulin. Rekomandimet dhe portofoli MBETEN (analizë e të
    dhënave historike, jo parashikim), me një shënim të qartë se s'janë këshillë financiare.

Nxjerr në ./site: index.html, kripto.html, rekomandime.html, portofoli.html + assets.
"""

from __future__ import annotations

import glob
import html
import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("frozen")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)
DATA_DIR = os.path.join(PROJECT, "data", "crypto") + "/"
SITE = os.path.join(HERE, "site")

# Importet e moduleve të projektit — pandas i pastër, pa Spark.
sys.path.insert(0, PROJECT)
from investment_recommender import InvestmentRecommender  # noqa: E402
from portfolio_optimizer import PortfolioOptimizer  # noqa: E402


def load_all() -> pd.DataFrame:
    """Të gjitha snapshot-et në një DataFrame. Ngarkohet NJË herë e ndahet me modulet."""
    files = glob.glob(f"{DATA_DIR}date=*/**/*.parquet", recursive=True)
    log.info("po lexohen %d skedarë parquet…", len(files))
    frames = []
    for path in files:
        try:
            df = pd.read_parquet(path)
            if not df.empty:
                frames.append(df)
        except Exception:
            continue
    if not frames:
        raise SystemExit("s'u gjet asnjë parquet — a është montuar data/crypto?")
    data = pd.concat(frames, ignore_index=True)
    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
    data = data.dropna(subset=["timestamp", "symbol", "price"])
    log.info("gjithsej %d rreshta, %d monedha, %s → %s",
             len(data), data["symbol"].nunique(),
             data["timestamp"].min(), data["timestamp"].max())
    return data


def latest_per_coin(data: pd.DataFrame) -> pd.DataFrame:
    """Snapshot-i më i fundit për çdo monedhë — çka tregojnë tabelat dhe stat-et."""
    latest = data.sort_values("timestamp").groupby("symbol").tail(1)
    return latest.sort_values("market_cap", ascending=False).reset_index(drop=True)


# ── analiza, e drejtuar nga modulet e projektit ───────────────────────────
def run_modules(data: pd.DataFrame) -> tuple[dict, dict]:
    """
    Thërret InvestmentRecommender dhe PortfolioOptimizer, por i detyron të përdorin
    DataFrame-in e ngarkuar tashmë — përndryshe secili do të riskanonte 15 mijë skedarë.
    Kjo është i vetmja "ndërhyrje": logjika e vlerësimit mbetet e tyrja.
    """
    cached = data.copy()

    rec = InvestmentRecommender()
    rec.data_dir = DATA_DIR
    rec.load_crypto_data = lambda *a, **k: cached.copy()  # type: ignore

    recommendations = {}
    for profile in ("conservative", "balanced", "aggressive"):
        try:
            recommendations[profile] = rec.get_portfolio_recommendations(profile, top_n=30)[:15]
        except Exception as err:
            log.warning("rekomandime %s dështoi: %s", profile, err)
            recommendations[profile] = []

    opt = PortfolioOptimizer()
    opt.data_dir = DATA_DIR
    opt.load_crypto_data = lambda *a, **k: cached.copy()  # type: ignore

    try:
        portfolio = opt.create_simple_portfolio(top_n=15) or {}
    except Exception as err:
        log.warning("portofoli dështoi: %s", err)
        portfolio = {}

    return recommendations, portfolio


# ── HTML ───────────────────────────────────────────────────────────────────
NAV = [
    ("index.html", "Përmbledhje"),
    ("kripto.html", "Kriptovalutat"),
    ("rekomandime.html", "Rekomandime"),
    ("portofoli.html", "Portofoli"),
]


def shell(title: str, active: str, body: str, meta: dict, scripts: str = "") -> str:
    nav = "".join(
        f'<a href="{href}" class="{"on" if href == active else ""}">{html.escape(label)}</a>'
        for href, label in NAV
    )
    return f"""<!doctype html>
<html lang="sq"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex">
<title>{html.escape(title)} · Analitika e të dhënave</title>
<link rel="stylesheet" href="assets/style.css">
<script src="assets/chart.min.js"></script>
</head><body>
<header>
  <div class="brand">SpaceData<span>·analitikë</span></div>
  <nav>{nav}</nav>
</header>
<div class="frozen">
  ❄️ Panel i <b>ngrirë</b> — të dhëna historike nga {meta['start']} deri {meta['end']}
  ({meta['snapshots']:,} snapshot-e, {meta['coins']} monedha). Mbledhja e të dhënave është
  e ndalur; asgjë s'përditësohet.
</div>
<main>{body}</main>
<footer>
  Vetëm për informim — <b>nuk është këshillë financiare</b>. Të dhënat: CoinGecko, të ngrira
  më {meta['built']}. · SpaceData
</footer>
{scripts}
</body></html>"""


def stat_tile(label: str, value: str, sub: str = "") -> str:
    return (f'<div class="tile"><div class="v">{html.escape(value)}</div>'
            f'<div class="l">{html.escape(label)}</div>'
            f'{f"<div class=s>{html.escape(sub)}</div>" if sub else ""}</div>')


def fmt_money(x: float) -> str:
    if x >= 1e9:
        return f"${x/1e9:.2f}B"
    if x >= 1e6:
        return f"${x/1e6:.2f}M"
    if x >= 1e3:
        return f"${x/1e3:.2f}K"
    return f"${x:,.2f}"


def fmt_price(x: float) -> str:
    return f"${x:,.2f}" if x >= 1 else f"${x:.6f}"


def build_index(latest: pd.DataFrame, meta: dict) -> str:
    total_mcap = latest["market_cap"].sum()
    total_vol = latest["volume"].sum()
    avg_change = latest["change_24h"].mean()

    tiles = "".join([
        stat_tile("Monedha të gjurmuara", f"{len(latest):,}"),
        stat_tile("Kapitalizim total", fmt_money(total_mcap)),
        stat_tile("Volum 24h", fmt_money(total_vol)),
        stat_tile("Ndryshim mesatar 24h", f"{avg_change:+.2f}%"),
    ])

    top = latest.head(15)
    gainers = latest.sort_values("change_24h", ascending=False).head(8)
    losers = latest.sort_values("change_24h").head(8)

    data = {
        "mcapLabels": top["symbol"].tolist(),
        "mcapValues": (top["market_cap"] / 1e9).round(3).tolist(),
        "gainLabels": gainers["symbol"].tolist(),
        "gainValues": gainers["change_24h"].round(2).tolist(),
        "loseLabels": losers["symbol"].tolist(),
        "loseValues": losers["change_24h"].round(2).tolist(),
    }

    body = f"""
    <h1>Përmbledhje e tregut</h1>
    <div class="tiles">{tiles}</div>
    <div class="card"><h2>15 monedhat më të mëdha (kapitalizim, miliardë $)</h2>
      <canvas id="mcap" height="120"></canvas></div>
    <div class="grid2">
      <div class="card"><h2>Rritësit më të mëdhenj (24h)</h2><canvas id="gain" height="200"></canvas></div>
      <div class="card"><h2>Rënësit më të mëdhenj (24h)</h2><canvas id="lose" height="200"></canvas></div>
    </div>"""

    scripts = f"""<script>const D={json.dumps(data)};
    new Chart(mcap,{{type:'bar',data:{{labels:D.mcapLabels,datasets:[{{label:'Mrd $',data:D.mcapValues,backgroundColor:'#4f7cff'}}]}},options:{{plugins:{{legend:{{display:false}}}}}}}});
    new Chart(gain,{{type:'bar',data:{{labels:D.gainLabels,datasets:[{{data:D.gainValues,backgroundColor:'#16a34a'}}]}},options:{{indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
    new Chart(lose,{{type:'bar',data:{{labels:D.loseLabels,datasets:[{{data:D.loseValues,backgroundColor:'#dc2626'}}]}},options:{{indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
    </script>"""
    return shell("Përmbledhje", "index.html", body, meta, scripts)


def build_crypto(data: pd.DataFrame, latest: pd.DataFrame, meta: dict) -> str:
    rows = []
    for _, r in latest.head(100).iterrows():
        cls = "up" if r["change_24h"] >= 0 else "down"
        rows.append(
            f"<tr><td>{int(r['rank']) if pd.notna(r['rank']) else '—'}</td>"
            f"<td class=name>{html.escape(str(r['name']))} <span>{html.escape(str(r['symbol']))}</span></td>"
            f"<td>{fmt_price(r['price'])}</td>"
            f"<td>{fmt_money(r['market_cap'])}</td>"
            f"<td>{fmt_money(r['volume'])}</td>"
            f"<td class={cls}>{r['change_24h']:+.2f}%</td></tr>"
        )

    # Seri kohore për 5 monedhat kryesore, të grumbulluara sipas ore — një bosht kategorish
    # me etiketa të përbashkëta, që s'kërkon adapter date-time për Chart.js.
    top5 = latest.head(5)["symbol"].tolist()
    hourly = (data[data["symbol"].isin(top5)]
              .assign(hour=lambda d: d["timestamp"].dt.strftime("%d.%m %H:00"))
              .groupby(["symbol", "hour"])["price"].mean().reset_index())
    hours = sorted(hourly["hour"].unique())
    series = []
    for sym in top5:
        by_hour = dict(zip(hourly[hourly["symbol"] == sym]["hour"],
                           hourly[hourly["symbol"] == sym]["price"]))
        series.append({
            "label": sym,
            "data": [round(float(by_hour[h]), 6) if h in by_hour else None for h in hours],
        })

    body = f"""
    <h1>Kriptovalutat</h1>
    <div class="card"><h2>Çmimi ndër kohë — 5 monedhat kryesore</h2>
      <canvas id="ts" height="110"></canvas></div>
    <div class="card"><h2>Snapshot-i më i fundit ({len(latest)} monedha, top 100)</h2>
    <table class="data"><thead><tr><th>#</th><th>Monedha</th><th>Çmimi</th>
      <th>Kapitalizim</th><th>Volum 24h</th><th>24h</th></tr></thead>
    <tbody>{''.join(rows)}</tbody></table></div>"""

    scripts = f"""<script>const S={json.dumps(series)},H={json.dumps(hours)};
    const colors=['#4f7cff','#16a34a','#f59e0b','#ec4899','#06b6d4'];
    new Chart(ts,{{type:'line',data:{{labels:H,datasets:S.map((s,i)=>({{label:s.label,data:s.data,
      borderColor:colors[i%colors.length],borderWidth:2,pointRadius:0,tension:.25,spanGaps:true}}))}},
      options:{{scales:{{y:{{type:'logarithmic'}}}}}}}});</script>"""
    return shell("Kriptovalutat", "kripto.html", body, meta, scripts)


def build_recommendations(recs: dict, meta: dict) -> str:
    sections = []
    labels = {"conservative": "Konservator", "balanced": "I balancuar", "aggressive": "Agresiv"}
    for profile, items in recs.items():
        if not items:
            continue
        rows = []
        for it in items:
            sc = it["scores"]
            cls = "up" if it["change_24h"] >= 0 else "down"
            rows.append(
                f"<tr><td class=name>{html.escape(str(it['name']))} "
                f"<span>{html.escape(str(it['symbol']))}</span></td>"
                f"<td>{html.escape(str(it['category']))}</td>"
                f"<td class=score>{sc['investment_score']:.0f}</td>"
                f"<td>{sc['stability']:.0f}</td><td>{sc['growth_potential']:.0f}</td>"
                f"<td>{sc['liquidity_risk']:.0f}</td>"
                f"<td>{html.escape(str(it['recommended_allocation']))}</td>"
                f"<td class={cls}>{it['change_24h']:+.2f}%</td></tr>"
            )
        sections.append(
            f"<div class='card'><h2>Profili: {labels.get(profile, profile)}</h2>"
            f"<table class='data'><thead><tr><th>Monedha</th><th>Kategoria</th>"
            f"<th>Pikë</th><th>Stabilitet</th><th>Rritje</th><th>Risk likuiditeti</th>"
            f"<th>Alokim</th><th>24h</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
        )

    note = ("<div class='note'>Pikët llogariten nga të dhënat historike (stabilitet, "
            "momentum, likuiditet). Kjo është analizë e së kaluarës — <b>jo parashikim dhe "
            "jo këshillë investimi</b>.</div>")
    body = f"<h1>Rekomandime nga të dhënat</h1>{note}{''.join(sections) or '<p>S’ka mjaftueshëm të dhëna.</p>'}"
    return shell("Rekomandime", "rekomandime.html", body, meta)


def build_portfolio(portfolio: dict, meta: dict) -> str:
    strategies = portfolio.get("portfolios", {}) if portfolio else {}
    cards, charts = [], []
    for i, (key, strat) in enumerate(strategies.items()):
        allocs = strat.get("allocations", [])
        rows = "".join(
            f"<tr><td class=name>{html.escape(str(a.get('name', a.get('symbol', '—'))))}</td>"
            f"<td>{a['allocation']:.2f}%</td></tr>" for a in allocs[:10]
        )
        cid = f"pie{i}"
        cards.append(f"""<div class="card">
          <h2>{html.escape(str(strat.get('name', key)))}
            <span class="tag">{html.escape(str(strat.get('risk_level', '')))}</span></h2>
          <div class="pf"><canvas id="{cid}" height="200"></canvas>
          <div><p>Kthim i pritur: <b>{strat.get('expected_return', 0):.2f}%</b><br>
          Luhatshmëri: <b>{strat.get('volatility', 0):.2f}%</b></p>
          <table class="data"><thead><tr><th>Aseti</th><th>Alokim</th></tr></thead>
          <tbody>{rows}</tbody></table></div></div></div>""")
        charts.append(
            f"new Chart({cid},{{type:'doughnut',data:{{labels:{json.dumps([a.get('name', a.get('symbol','')) for a in allocs[:10]])},"
            f"datasets:[{{data:{json.dumps([round(a['allocation'],2) for a in allocs[:10]])},"
            f"backgroundColor:['#4f7cff','#16a34a','#f59e0b','#ec4899','#06b6d4','#8b5cf6','#ef4444','#14b8a6','#f97316','#a3a3a3']}}]}},"
            f"options:{{plugins:{{legend:{{position:'right',labels:{{boxWidth:12}}}}}}}}}});"
        )

    note = ("<div class='note'>Alokim sipas Teorisë Moderne të Portofolit (Markowitz) mbi të "
            "dhënat historike. <b>Ilustrim, jo këshillë investimi.</b></div>")
    body = (f"<h1>Optimizim i portofolit</h1>{note}"
            f"{''.join(cards) or '<p>S’ka mjaftueshëm të dhëna për optimizim.</p>'}")
    scripts = f"<script>{''.join(charts)}</script>" if charts else ""
    return shell("Portofoli", "portofoli.html", body, meta, scripts)


CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;background:#0b1020;color:#e8ecf6;line-height:1.5}
header{display:flex;align-items:center;gap:28px;padding:16px 28px;background:#0e1428;border-bottom:1px solid #1e2947;position:sticky;top:0;z-index:5}
.brand{font-weight:800;font-size:1.15rem}.brand span{color:#4f7cff;font-weight:600;font-size:.85rem}
nav{display:flex;gap:18px}nav a{color:#9aa6bd;text-decoration:none;font-weight:500;padding:4px 2px;border-bottom:2px solid transparent}
nav a:hover{color:#fff}nav a.on{color:#fff;border-color:#4f7cff}
.frozen{background:#13284a;color:#bcd3ff;font-size:.9rem;padding:10px 28px;border-bottom:1px solid #1e2947}
main{max-width:1080px;margin:0 auto;padding:28px 22px}
h1{font-size:1.8rem;letter-spacing:-.02em;margin-bottom:20px}
h2{font-size:1.05rem;margin-bottom:14px;color:#c7cfe0}h2 .tag{font-size:.72rem;background:#1e2947;color:#8fa0ff;padding:2px 8px;border-radius:6px;margin-left:6px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px;margin-bottom:22px}
.tile{background:#141c33;border:1px solid #24304f;border-radius:12px;padding:18px}
.tile .v{font-size:1.6rem;font-weight:700}.tile .l{color:#9aa6bd;font-size:.85rem;margin-top:4px}.tile .s{color:#6b7794;font-size:.75rem}
.card{background:#141c33;border:1px solid #24304f;border-radius:14px;padding:20px;margin-bottom:20px;overflow-x:auto}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:20px}
@media(max-width:760px){.grid2{grid-template-columns:1fr}}
table.data{width:100%;border-collapse:collapse;font-size:.9rem}
table.data th{text-align:left;color:#8fa0ff;font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.04em;padding:8px 10px;border-bottom:1px solid #24304f}
table.data td{padding:8px 10px;border-bottom:1px solid #1a2340}
td.name{font-weight:600}td.name span{color:#6b7794;font-weight:400;font-size:.8rem;margin-left:4px}
td.score{font-weight:700;color:#4f7cff}td.up{color:#22c55e}td.down{color:#ef4444}
.pf{display:grid;grid-template-columns:240px 1fr;gap:20px;align-items:center}
@media(max-width:640px){.pf{grid-template-columns:1fr}}
.note{background:#1a2036;border-left:3px solid #f59e0b;padding:12px 16px;border-radius:8px;font-size:.88rem;color:#c7cfe0;margin-bottom:20px}
footer{max-width:1080px;margin:0 auto;padding:24px 22px;color:#6b7794;font-size:.82rem;border-top:1px solid #1e2947}
"""


def main() -> int:
    os.makedirs(os.path.join(SITE, "assets"), exist_ok=True)

    data = load_all()
    latest = latest_per_coin(data)
    recs, portfolio = run_modules(data)

    meta = {
        "start": data["timestamp"].min().strftime("%d.%m.%Y %H:%M"),
        "end": data["timestamp"].max().strftime("%d.%m.%Y %H:%M"),
        "snapshots": int(len(data)),
        "coins": int(latest["symbol"].nunique()),
        "built": datetime.now().strftime("%d.%m.%Y"),
    }

    pages = {
        "index.html": build_index(latest, meta),
        "kripto.html": build_crypto(data, latest, meta),
        "rekomandime.html": build_recommendations(recs, meta),
        "portofoli.html": build_portfolio(portfolio, meta),
    }
    for name, content in pages.items():
        with open(os.path.join(SITE, name), "w") as f:
            f.write(content)
        log.info("shkrova %s (%d KB)", name, len(content) // 1024)

    with open(os.path.join(SITE, "assets", "style.css"), "w") as f:
        f.write(CSS)

    log.info("përfundoi → %s", SITE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
