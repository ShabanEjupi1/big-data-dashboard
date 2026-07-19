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


# ── matje mbi korpusin, për faqen e metodologjisë ─────────────────────────
#
# Faqja e metodologjisë nuk PRETENDON se Parquet-i kursen hapësirë — e MAT, mbi
# këtë korpus, sa herë që ndërtohet paneli. Një punim që thotë "Parquet-i është
# më i vogël" pa numra është opinion; tabela e mëposhtme është provë e riprodhueshme.
def measure_corpus(data: pd.DataFrame) -> dict:
    """
    Numëron skedarët/ndarjet në disk dhe rishkruan të njëjtat të dhëna në disa
    formate për t'i krahasuar madhësitë. Punon në një dosje të përkohshme dhe nuk
    prek kurrë korpusin origjinal.
    """
    import tempfile

    files = glob.glob(f"{DATA_DIR}date=*/**/*.parquet", recursive=True)
    on_disk = sum(os.path.getsize(f) for f in files)
    partitions = {os.path.dirname(f) for f in files}
    dates = {p.split("date=")[1].split("/")[0] for p in partitions if "date=" in p}

    # Bajtat e parquet-it janë vetëm gjysma e historisë. Spark-u lë pas një skedar
    # kontrolli .crc për ÇDO skedar, dhe sistemi i skedarëve nuk alokon dot më pak se
    # një bllok (zakonisht 4 KB) — kështu që një copë 2.8 KB harxhon 4 KB. st_blocks
    # numëron blloqet e alokuara vërtet, pra e mat hapësirën e shpenzuar, jo atë të
    # deklaruar. Kjo është arsyeja pse `du` raporton disa herë më shumë se shuma e
    # madhësive, dhe është pjesë e kostos reale të skedarëve të vegjël.
    # os.walk, JO glob: glob-i i kapërcen skedarët që nisin me pikë, dhe skedarët .crc
    # të Spark-ut janë pikërisht të tillë. Me glob dilnin 37 skedarë shoqërues në vend
    # të mbi 16 mijë — pra pikërisht kostoja që ky seksion do të masë do të mbetej e
    # padukshme.
    sidecars, allocated, apparent_all = 0, 0, 0
    for root, _dirs, names in os.walk(DATA_DIR):
        for n in names:
            try:
                st = os.stat(os.path.join(root, n))
            except OSError:
                continue
            allocated += st.st_blocks * 512
            apparent_all += st.st_size
            if not n.endswith(".parquet"):
                sidecars += 1

    fmts: dict[str, int] = {}
    with tempfile.TemporaryDirectory() as tmp:
        def size(name: str, writer) -> None:
            path = os.path.join(tmp, name)
            try:
                writer(path)
                fmts[name] = os.path.getsize(path)
            except Exception as err:          # një motor që mungon s'duhet ta rrëzojë ndërtimin
                log.warning("matja e %s dështoi: %s", name, err)

        size("csv",            lambda p: data.to_csv(p, index=False))
        size("csv.gz",         lambda p: data.to_csv(p, index=False, compression="gzip"))
        size("json",           lambda p: data.to_json(p, orient="records"))
        size("parquet.snappy", lambda p: data.to_parquet(p, compression="snappy", index=False))
        size("parquet.gzip",   lambda p: data.to_parquet(p, compression="gzip", index=False))

        # Përfitimi i formatit KOLONOR: sa kushton të lexosh vetëm 2 kolona nga 10.
        # Në një format rreshtor duhet lexuar i tërë rreshti për të marrë një fushë.
        size("parquet.2col",   lambda p: data[["symbol", "price"]]
                                             .to_parquet(p, compression="snappy", index=False))

    cols = []
    for c in data.columns:
        s = data[c]
        cols.append({
            "name": c,
            "dtype": str(s.dtype),
            "nonnull": float(s.notna().mean() * 100),
            "distinct": int(s.nunique(dropna=True)),
        })

    return {
        "files": len(files),
        "on_disk": on_disk,
        "sidecars": sidecars,
        "allocated": allocated,
        "apparent_all": apparent_all,
        "partitions": len(partitions),
        "dates": len(dates),
        "avg_file": on_disk / len(files) if files else 0,
        "rows": len(data),
        "bytes_per_row_disk": on_disk / len(data) if len(data) else 0,
        "fmts": fmts,
        "cols": cols,
        "mem": int(data.memory_usage(deep=True).sum()),
    }


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
    ("metodologjia.html", "Metodologjia"),
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


# Përshkrimet e kolonave. Skema vjen nga të dhënat (tipi, mbushja, vlerat e dallueshme
# maten), por KUPTIMI i një kolone — njësia, pse mbahet, ku përdoret — nuk gjendet dot
# në skedar. Kjo tabelë është ajo pjesë, dhe është pikërisht ajo që një punim kërkon.
COLUMN_DOCS = {
    "id":         ("varg", "—", "Identifikuesi unik i monedhës te CoinGecko (<code>bitcoin</code>). "
                              "Çelësi i vetëm i qëndrueshëm: simbolet përsëriten mes projekteve, emrat ndryshojnë."),
    "symbol":     ("varg", "—", "Tikeri i shkurtër me shkronja të mëdha (<code>BTC</code>). Çelësi i grupimit "
                              "në të gjitha analizat dhe etiketa në grafikë."),
    "name":       ("varg", "—", "Emri i lexueshëm (<code>Bitcoin</code>). Vetëm paraqitje — nuk hyn kurrë në llogaritje."),
    "price":      ("dhjetor", "USD", "Çmimi i çastit në dollarë. Ndryshorja qendrore: prej saj dalin seritë kohore, "
                              "luhatshmëria dhe kthimet."),
    "market_cap": ("dhjetor", "USD", "Kapitalizimi i tregut = çmimi × oferta qarkulluese. Masa e madhësisë; "
                              "përdoret për renditje dhe si peshë në portofol."),
    "volume":     ("dhjetor", "USD", "Volumi i tregtuar në 24 orë. Përdoret si masë <b>likuiditeti</b> — sa lehtë "
                              "shitet një pozicion pa lëvizur çmimin."),
    "change_24h": ("dhjetor", "%", "Ndryshimi i çmimit në 24 orë. Sinjali i <b>momentum</b>-it dhe, mbi shumë "
                              "snapshot-e, lënda e parë e luhatshmërisë."),
    "rank":       ("i plotë", "—", "Rangu sipas kapitalizimit (1 = më i madhi). I tepërt ndaj <code>market_cap</code>, "
                              "por mbahet sepse është si e jep burimi — të dhënat ruhen ashtu siç mbërrijnë."),
    "timestamp":  ("kohë", "ISO-8601", "Momenti i mbledhjes. Boshti kohor i gjithçkaje, dhe fusha nga e cila "
                              "rrjedhin ndarjet <code>date=</code>/<code>hour=</code>."),
    "source":     ("varg", "—", "Prejardhja: <code>coingecko</code> ose <code>binance</code>. Mbahet që një rresht "
                              "të mund t'i kthehet burimit të vet dhe që burimet të krahasohen."),
}

FMT_DOCS = {
    "json":           ("JSON (records)", "Formati në të cilin mbërrin nga API-ja. Çdo rresht mbart sërish "
                                         "emrat e të gjitha fushave — tekst i pastër, zero kompresim."),
    "csv":            ("CSV", "Tabelor, rreshtor, pa tipa: çdo numër bëhet tekst dhe kush e lexon duhet ta "
                              "hamendësojë tipin."),
    "csv.gz":         ("CSV + gzip", "I njëjti CSV i kompresuar. Bie shumë, por mbetet <b>i pandashëm</b> — "
                                     "gzip-i s'lexohet dot nga mesi, pra një skedar = një task."),
    "parquet.snappy": ("Parquet + Snappy", "Kolonor, me skemë brenda, i ndashëm. Kjo është ajo që përdoret."),
    "parquet.gzip":   ("Parquet + gzip", "I njëjti Parquet me kompresim më agresiv: më i vogël, më i ngadaltë "
                                         "për t'u lexuar. Snappy zgjidhet për shpejtësi."),
}


def build_methodology(data: pd.DataFrame, m: dict, meta: dict) -> str:
    """Kapitulli i metodologjisë: nga API-ja te paneli, me numra të matur, jo të pohuar."""

    def mb(n: float) -> str:
        return f"{n/1e6:.1f} MB" if n >= 1e6 else (f"{n/1e3:.1f} KB" if n >= 1e3 else f"{n:.0f} B")

    # ── 1. tubacioni ──
    pipeline = """
    <div class="pipe">
      <div class="step"><b>1 · Mbledhja</b><span>CoinGecko + Binance REST<br>çdo disa minuta</span></div>
      <div class="step"><b>2 · Ruajtja</b><span>Parquet i ndarë<br>date= / hour=</span></div>
      <div class="step"><b>3 · Përpunimi</b><span>Spark mbi 10 VM<br>(pandas në ngrirje)</span></div>
      <div class="step"><b>4 · Analiza</b><span>vlerësim + Markowitz</span></div>
      <div class="step"><b>5 · Paraqitja</b><span>HTML statik</span></div>
    </div>"""

    # ── 2. kolonat ──
    crows = []
    for c in m["cols"]:
        typ, unit, desc = COLUMN_DOCS.get(c["name"], ("—", "—", ""))
        crows.append(
            f"<tr><td class=name><code>{html.escape(c['name'])}</code></td>"
            f"<td>{typ}</td><td>{unit}</td>"
            f"<td class=num>{c['distinct']:,}</td>"
            f"<td class=num>{c['nonnull']:.1f}%</td>"
            f"<td class=desc>{desc}</td></tr>"
        )

    # ── 3. formatet ──
    base = m["fmts"].get("parquet.snappy")
    frows = []
    # Renditur nga më i madhi te më i vogli — jo me të zgjedhurin në fund, sepse i
    # zgjedhuri NUK është më i vogli, dhe tabela s'duhet ta fshehë këtë.
    for key in sorted((k for k in m["fmts"] if k in FMT_DOCS),
                      key=lambda k: -m["fmts"][k]):
        size_b = m["fmts"][key]
        label, note = FMT_DOCS[key]
        ratio = f"{size_b/base:.2f}×" if base else "—"
        cls = " class=win" if key == "parquet.snappy" else ""
        pick = " <span class='tag'>e zgjedhura</span>" if key == "parquet.snappy" else ""
        frows.append(
            f"<tr{cls}><td class=name>{label}{pick}</td><td class=num>{mb(size_b)}</td>"
            f"<td class=num>{ratio}</td><td class=desc>{note}</td></tr>"
        )

    # Nëse ndonjë format rreshtor doli më i vogël, thuaje hapur. Një punim që fsheh
    # matjen e vet të papërshtatshme s'ka pse t'i besohet për matjet e tjera.
    smaller = {k: v for k, v in m["fmts"].items()
               if base and v < base and k in FMT_DOCS and k != "parquet.snappy"}
    honesty = ""
    if smaller:
        names = ", ".join(f"<b>{FMT_DOCS[k][0]}</b> ({mb(v)})" for k, v in
                          sorted(smaller.items(), key=lambda kv: kv[1]))
        pct = (base / min(smaller.values()) - 1) * 100
        honesty = (
            f"<p class='after'><b>Dhe këtu matja nuk e mbështet pritshmërinë:</b> {names} "
            f"dalin <i>më të vegjël</i> se e zgjedhura, Parquet+Snappy ({mb(base)}). Po ta "
            f"zgjidhnim formatin vetëm nga madhësia, do të zgjidhnim diçka tjetër — prandaj "
            f"madhësia nuk është kriteri i vetëm. Të dy humbësit humbin për arsye të ndryshme:</p>"
            f"<ul class='reasons'>"
            f"<li><b>CSV + gzip</b> s'ka fare garë: gzip-i është <b>i pandashëm</b> — për të "
            f"lexuar bajtin e fundit duhet shpaketuar që nga i pari, pra një skedar s'ndahet dot "
            f"mes dy ekzekutuesve. Bashkë me të bien edhe leximi i një kolone të vetme, kapërcimi "
            f"i grupeve me filtër dhe tipat e fushave. Është më i vogël dhe njëkohësisht i "
            f"papërdorshëm në shkallë.</li>"
            f"<li><b>Parquet + gzip</b> është kandidat i vërtetë: po aq i ndashëm dhe kolonor, "
            f"vetëm ~{pct:.0f}% më i vogël. Humb <i>vetëm</i> te shpejtësia — gzip-i dekompresohet "
            f"disa herë më ngadalë se Snappy. Për të dhëna që shkruhen njëherë dhe lexohen shpesh, "
            f"kohë CPU-je në çdo lexim kushton më shumë se {mb(base - min(smaller.values()))} disk "
            f"një herë të vetme. Ky është kompromis i vetëdijshëm, jo gabim.</li>"
            f"</ul>")

    twocol = m["fmts"].get("parquet.2col")
    proj = ""
    if twocol and base:
        proj = (f"<p class='after'>Dhe përfitimi që s'duket në asnjë rresht të tabelës: "
                f"leximi i vetëm dy kolonave (<code>symbol</code>, <code>price</code>) prek "
                f"<b>{mb(twocol)}</b> në vend të <b>{mb(base)}</b> — "
                f"<b>{100 - twocol/base*100:.0f}% më pak</b> nga disku, sepse një format kolonor "
                f"i ruan kolonat veç e veç dhe thjesht i kapërcen ato që s'i kërkove. Në CSV ose "
                f"JSON e njëjta pyetje lexon çdo bajt të çdo rreshti.</p>")

    # ── 4. skedarët e vegjël ──
    inflation = m["on_disk"] / base if base else 0
    small = f"""
    <div class="tiles">
      {stat_tile("Skedarë parquet", f"{m['files']:,}", "një për çdo shkrim Spark")}
      {stat_tile("Madhësia mesatare", mb(m['avg_file']), "shumë nën bllokun 128 MB")}
      {stat_tile("Në disk gjithsej", mb(m['on_disk']), f"për {m['rows']:,} rreshta")}
      {stat_tile("I njëjti korpus, i ngjeshur", mb(base or 0), f"{inflation:.0f}× më i vogël")}
    </div>
    <p>Ky është <b>problemi i skedarëve të vegjël</b>, i kapur në vetë korpusin e këtij punimi.
    Mbledhësi shkruan me <code>mode("append")</code> në çdo grumbull, dhe çdo shkrim prodhon një
    skedar <code>part-*</code> për çdo ndarje Spark. Rezultati: <b>{m['files']:,}</b> skedarë me
    mesatare <b>{mb(m['avg_file'])}</b>, kur blloku i HDFS-së është 128 MB.</p>
    <p>Kostoja nuk është hapësira në vetvete — është se <b>çdo skedar mbart footer-in, skemën dhe
    statistikat e veta</b>. Me pak rreshta për skedar, ai footer e tejkalon përmbajtjen: i njëjti
    grup të dhënash, i shkruar si <b>një</b> skedar Parquet, zë <b>{mb(base or 0)}</b> — rreth
    <b>{inflation:.0f}× më pak</b> se <b>{mb(m['on_disk'])}</b> që zënë {m['files']:,} copat.
    Në disk secili rresht kushton <b>{m['bytes_per_row_disk']/1e3:.1f} KB</b>; i ngjeshur, nën
    {base/m['rows']:.0f} bajt.</p>
    <p>Edhe leximi vuan njësoj: {m['files']:,} hapje skedari kërkojnë {m['files']:,} raunde
    metadatash, dhe Spark-u nis një task për skedar — pra dhjetëra mijëra task-e që secili bën
    thuajse asgjë. Ilaçi standard është <b>ngjeshja</b> (<code>coalesce</code>/<code>repartition</code>
    para shkrimit, ose një punë periodike ngjeshjeje). Ky panel e shmang problemin nga ana tjetër:
    e lexon korpusin një herë dhe nxjerr HTML statik.</p>

    <h3>Kostoja e fshehtë: blloqet e sistemit të skedarëve</h3>
    <p>Shuma e madhësive nuk është hapësira që harxhohet vërtet. Spark-u shkruan edhe një skedar
    kontrolli <code>.crc</code> për çdo skedar të dhënash — <b>{m['sidecars']:,}</b> skedarë
    shoqërues këtu — dhe sistemi i skedarëve nuk alokon dot më pak se një bllok, zakonisht 4 KB.
    Një copë {mb(m['avg_file'])} zë <b>një bllok të plotë</b>; pjesa tjetër e bllokut humbet.</p>
    <p>Prandaj {mb(m['apparent_all'])} të deklaruara zënë <b>{mb(m['allocated'])}</b> në disk —
    <b>{m['allocated']/m['apparent_all']:.1f}× më shumë</b>, dhe kjo është shifra që sheh
    <code>du</code>. E zinxhiruar me shifrat e mësipërme, rruga nga {mb(base or 0)} te
    {mb(m['allocated'])} është një amplifikim prej rreth
    <b>{m['allocated']/base if base else 0:.0f}×</b>, i shkaktuar tërësisht nga mënyra e shkrimit
    — jo nga sasia e informacionit. Vetë korpusi i këtij punimi është demonstrimi më i mirë i
    problemit që ai përshkruan.</p>"""

    body = f"""
    <h1>Metodologjia</h1>
    <div class="note">Kjo faqe dokumenton <b>si</b> u ndërtua ky panel: nga vjen çdo kolonë, pse
    të dhënat ruhen në Parquet, si janë të ndara, dhe çfarë llogaritin modulet. Numrat më poshtë
    <b>maten gjatë çdo ndërtimi</b> mbi korpusin real — nuk janë të shkruar me dorë.</div>

    <div class="card"><h2>1 · Tubacioni</h2>
      {pipeline}
      <p class="after">Pesë faza, secila me një përgjegjësi. Mbledhja s'di gjë për analizën;
      analiza s'di gjë për HTML-në. Kjo është arsyeja pse faza 3 mundi të zëvendësohet — Spark
      me pandas — pa prekur asnjë rresht të fazës 4.</p>
    </div>

    <div class="card"><h2>2 · Burimi i të dhënave</h2>
      <p>Burimi parësor është <b>CoinGecko</b> (<code>/coins/markets</code>, 250 monedhat më të
      mëdha për thirrje), me <b>Binance</b> si burim dytësor për çiftet USDT. Të dyja janë publike
      dhe pa çelës, çka e bën korpusin të riprodhueshëm nga kushdo që lexon punimin.</p>
      <p>Korpusi mbulon <b>{meta['coins']} monedha</b> në <b>{m['rows']:,} rreshta</b>, nga
      {meta['start']} deri {meta['end']} — {m['dates']} ditë, {m['partitions']:,} ndarje orësh.
      Mbledhja tani është <b>e ndalur</b>: paneli është i ngrirë me qëllim.</p>
      <p class="after"><b>Vetëm kripto.</b> Mbledhësi ka edhe rrugë për aksione dhe Reddit, por ato
      dolën bosh — kufij thirrjesh dhe çelësa që munguan — dhe një kolonë bosh është më keq se
      asnjë kolonë. Analiza mbulon vetëm atë që u mblodh vërtet.</p>
    </div>

    <div class="card"><h2>3 · Skema — kolonat e marra në analizë</h2>
      <p>Dhjetë kolona për rresht. Tipi, vlerat e dallueshme dhe mbushja maten nga korpusi;
      njësia dhe qëllimi janë vendime të projektimit dhe dokumentohen këtu.</p>
      <table class="data"><thead><tr><th>Kolona</th><th>Tipi</th><th>Njësia</th>
        <th>Të dallueshme</th><th>E mbushur</th><th>Çfarë është dhe pse mbahet</th></tr></thead>
      <tbody>{''.join(crows)}</tbody></table>
      <p class="after">Një rresht = një monedhë në një çast. Çelësi natyror është
      (<code>id</code>, <code>timestamp</code>); <code>symbol</code> është çelësi praktik i
      grupimit. Të gjitha llogaritjet — luhatshmëri, momentum, likuiditet — dalin nga katër fushat
      numerike; të tjerat janë identitet, kohë dhe prejardhje.</p>
    </div>

    <div class="card"><h2>4 · Pse Parquet</h2>
      <p>I njëjti korpus, i shkruar në pesë formate gjatë këtij ndërtimi. Kolona e fundit është
      raporti ndaj Parquet+Snappy:</p>
      <table class="data"><thead><tr><th>Formati</th><th>Madhësia</th><th>Ndaj Parquet</th>
        <th>Vërejtje</th></tr></thead><tbody>{''.join(frows)}</tbody></table>
      {honesty}
      {proj}
      <p>Përtej madhësisë, arsyet që e bëjnë Parquet-in standardin e ekosistemit big-data:</p>
      <ul class="reasons">
        <li><b>Kolonor.</b> Kolonat ruhen veç. Një pyetje që prek 2 nga 10 kolona lexon rreth 2/10
            të të dhënave, jo gjithçka.</li>
        <li><b>Kompresim për kolonë.</b> Vlerat e një kolone janë të ngjashme dhe të njëtipta —
            mijëra <code>coingecko</code> radhazi ngjeshen thuajse në asgjë. Në një format rreshtor
            ato vlera qëndrojnë të shpërndara mes fushash të palidhura.</li>
        <li><b>Skema brenda skedarit.</b> Tipat udhëtojnë me të dhënat. Një CSV e humb këtë:
            <code>rank</code> kthehet në tekst dhe kush e lexon duhet ta rihamendësojë.</li>
        <li><b>I ndashëm.</b> Një skedar Parquet lexohet nga mesi, prandaj Spark-u e ndan mes
            ekzekutuesve. Një <code>.csv.gz</code> — sado i vogël — duhet të shpaketohet nga fillimi,
            pra e detyron një bërthamë të vetme.</li>
        <li><b>Statistika min/max për grup rreshtash.</b> Lejon <i>predicate pushdown</i>: një filtër
            mbi çmimin i kapërcen grupet ku çmimi s'bie kurrë brenda intervalit, pa i lexuar fare.</li>
      </ul>
    </div>

    <div class="card"><h2>5 · Ndarja (partitioning)</h2>
      <p>Të dhënat shkruhen në <code>data/crypto/date=YYYY-MM-DD/hour=HH/</code> — ndarje Hive-style,
      ku vlera ndodhet në <b>emrin e dosjes</b>, jo brenda skedarit. Fituar prej saj:</p>
      <ul class="reasons">
        <li><b>Krasitje ndarjesh.</b> Një pyetje për një ditë të vetme lexon një dosje. Motori as
            që i sheh {m['partitions']:,} ndarjet e tjera.</li>
        <li><b>Shtim pa rishkrim.</b> Një grumbull i ri krijon një dosje të re; asgjë ekzistuese nuk
            preket, prandaj mbledhja s'ka nevojë të bllokojë asgjë.</li>
        <li><b>Kolona falas.</b> <code>date</code> dhe <code>hour</code> lexohen nga shtegu — s'zënë
            asnjë bajt në skedar, por filtrohen si kolona normale.</li>
      </ul>
      <p class="after">Granulariteti orë-për-orë është edhe shkaku i drejtpërdrejtë i seksionit
      vijues: sa më e imët ndarja, aq më shumë skedarë — dhe aq më të vegjël.</p>
    </div>

    <div class="card"><h2>6 · Problemi i skedarëve të vegjël</h2>
      {small}
    </div>

    <div class="card"><h2>7 · Përpunimi dhe analiza</h2>
      <p>Mbledhja dhe përpunimi fillestar u bënë me <b>PySpark mbi një grup prej 10 makinash
      virtuale</b>. Për panelin e ngrirë, i njëjti korpus lexohet një herë me <b>pandas</b>:
      me {m['rows']:,} rreshta ({mb(m['mem'])} në memorie) të dhënat hyjnë rehat në një makinë,
      dhe Spark-u do të shtonte vetëm kohë nisjeje dhe RAM. <b>Spark-u fitoi te mbledhja
      paralele; pandas fiton te analiza njëhershe</b> — përzgjedhja e mjetit sipas madhësisë reale
      është vetë përfundim i punimit, jo lëshim.</p>
      <p>Mbi këtë DataFrame punojnë dy modulet:</p>
      <ul class="reasons">
        <li><b>Vlerësuesi</b> (<code>investment_recommender.py</code>) nxjerr për çdo monedhë
            pikë <i>stabiliteti</i> (e kundërta e luhatshmërisë së çmimit ndër snapshot-e),
            <i>potenciali rritës</i> (momentum mbi <code>change_24h</code>) dhe
            <i>risku i likuiditetit</i> (nga <code>volume</code> ndaj <code>market_cap</code>),
            dhe i peshon ndryshe sipas tri profileve të riskut.</li>
        <li><b>Optimizuesi</b> (<code>portfolio_optimizer.py</code>) ndërton alokime sipas
            <b>Teorisë Moderne të Portofolit (Markowitz)</b>: kthim i pritur kundrejt luhatshmërisë,
            duke përdorur kovariancën mes monedhave.</li>
      </ul>
    </div>

    <div class="card"><h2>8 · Kufizimet</h2>
      <ul class="reasons">
        <li><b>Dritare e shkurtër.</b> {m['dates']} ditë mbledhjeje. Mjaft për të treguar tubacionin
            dhe për statistika përshkruese; shumë pak për ndonjë pohim mbi sjelljen e tregut.</li>
        <li><b>Vetëm kripto.</b> Rrugët për aksione dhe Reddit mbetën bosh — pa krahasim mes klasave
            të aseteve.</li>
        <li><b>Anshmëri mbijetese.</b> Merren monedhat më të mëdha për kapitalizim <i>sot</i>;
            ato që u rrëzuan gjatë periudhës nuk hyjnë kurrë në kampion.</li>
        <li><b>Pa parashikim.</b> Paneli nuk parashikon çmime — faqja e parashikimeve u hoq me
            qëllim. Gjithçka këtu është analizë <b>përshkruese</b> e së kaluarës.</li>
        <li><b>Jo këshillë financiare.</b> Rekomandimet dhe portofoli janë ilustrim metodologjik.</li>
      </ul>
    </div>"""
    return shell("Metodologjia", "metodologjia.html", body, meta)


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

/* ── faqja e metodologjisë ── */
.card p{color:#b9c3d8;font-size:.92rem;margin-bottom:12px}
.card p.after{color:#9aa6bd;font-size:.88rem;margin-top:14px;margin-bottom:0}
.card code{background:#0e1428;border:1px solid #24304f;border-radius:4px;padding:1px 5px;font-size:.85em;color:#8fa0ff}
ul.reasons{margin:12px 0 0 18px;color:#b9c3d8;font-size:.92rem}
ul.reasons li{margin-bottom:9px;padding-left:4px}
ul.reasons li b{color:#e8ecf6}
.card h3{font-size:.95rem;color:#c7cfe0;margin:20px 0 10px}
/* Tubacioni: rrjedh horizontalisht në desktop, vertikalisht kur s'ka vend. */
.pipe{display:flex;flex-wrap:wrap;gap:10px;align-items:stretch;margin-bottom:6px}
.pipe .step{flex:1 1 150px;background:#0e1428;border:1px solid #24304f;border-radius:10px;padding:12px 14px;position:relative}
.pipe .step b{display:block;color:#8fa0ff;font-size:.8rem;margin-bottom:5px}
.pipe .step span{color:#9aa6bd;font-size:.8rem;line-height:1.45}
td.desc{color:#9aa6bd;font-size:.85rem;line-height:1.5}
td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
tr.win td{background:#12233d}
tr.win td.name{color:#7ee08a;font-weight:700}
"""


def main() -> int:
    os.makedirs(os.path.join(SITE, "assets"), exist_ok=True)

    data = load_all()
    latest = latest_per_coin(data)
    measured = measure_corpus(data)   # para moduleve: mat korpusin ashtu siç erdhi nga disku
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
        "metodologjia.html": build_methodology(data, measured, meta),
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
