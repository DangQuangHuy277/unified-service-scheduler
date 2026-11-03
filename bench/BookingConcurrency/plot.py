#!/usr/bin/env python3
"""Turn results.csv into the SVG charts used in docs/benchmarks (standard library only).

Usage: python3 plot.py results/full.csv ../../docs/benchmarks     (SVG charts)
       python3 plot.py --tables results/full.csv              (markdown tables)
"""
import csv
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

APPROACHES = [  # fixed order = fixed colour slot and marker, whatever the data contains
    ("o1-naive", "O1 naive", "circle"),
    ("o1-lock", "O1 + per-day lock", "square"),
    ("o2-single-writer", "O2 single writer", "triangle"),
]

STYLE = """
<style>
  .viz text { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; font-size: 12px; }
  .viz .title { font-size: 16px; font-weight: 600; }
  .viz .sub { font-size: 12.5px; }
  .viz .ptitle { font-size: 13px; font-weight: 600; }
  .viz .tick { font-size: 11px; font-variant-numeric: tabular-nums; }
  .viz .alabel, .viz .dlabel { font-size: 11.5px; }
  .viz .dlabel { font-weight: 500; }
  .viz .note { font-size: 11px; }
  .viz .grid, .viz .axis, .viz .ref { stroke-width: 1; }
  .viz .ref { stroke-dasharray: 4 4; fill: none; }
  .viz .line { stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }
  .viz .mark { stroke-width: 2; }
  .viz .swatch { stroke-width: 2; }
  .viz text { fill: #52514e; }
  .viz .title,   .viz .ptitle,   .viz .dlabel { fill: #0b0b0b; }
  .viz .sub,   .viz .alabel { fill: #52514e; }
  .viz .tick,   .viz .note { fill: #898781; }
  .viz .grid { stroke: #e1e0d9; }
  .viz .axis { stroke: #c3c2b7; }
  .viz .ref { stroke: #898781; }
  .viz .bg { fill: #fcfcfb; }
  .viz .s1 { stroke: #2a78d6; fill: #2a78d6; }
  .viz .s2 { stroke: #eb6834; fill: #eb6834; }
  .viz .s3 { stroke: #1baf7a; fill: #1baf7a; }
  .viz .line { fill: none; }
  .viz .mark { stroke: #fcfcfb; }
  @media (prefers-color-scheme: dark) {
    .viz text { fill: #c3c2b7; }
    .viz .title,     .viz .ptitle,     .viz .dlabel { fill: #f0efec; }
    .viz .sub,     .viz .alabel { fill: #c3c2b7; }
    .viz .tick,     .viz .note { fill: #898781; }
    .viz .grid { stroke: #2c2c2a; }
    .viz .axis { stroke: #383835; }
    .viz .ref { stroke: #898781; }
    .viz .bg { fill: #1a1a19; }
    .viz .s1 { stroke: #3987e5; fill: #3987e5; }
    .viz .s2 { stroke: #d95926; fill: #d95926; }
    .viz .s3 { stroke: #199e70; fill: #199e70; }
    .viz .line { fill: none; }
    .viz .mark { stroke: #1a1a19; }
  }
</style>
"""


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def nice_log_ticks(lo, hi):
    ticks, k = [], math.floor(math.log10(lo))
    while 10 ** k <= hi * 1.0001:
        for m in (1, 2, 5):
            v = m * 10 ** k
            if lo * 0.9999 <= v <= hi * 1.0001:
                ticks.append(v)
        k += 1
    return ticks


def nice_lin_ticks(lo, hi, n=5):
    span = hi - lo
    step = 10 ** math.floor(math.log10(span / n))
    for m in (1, 2, 2.5, 5, 10):
        if span / (m * step) <= n:
            step *= m
            break
    t, ticks = math.ceil(lo / step) * step, []
    while t <= hi + 1e-9:
        ticks.append(round(t, 10))
        t += step
    return ticks


def fmt(v):
    if v >= 1000:
        return f"{v / 1000:g}k"
    return f"{v:g}"


def marker(shape, x, y, cls, tip):
    t = f"<title>{esc(tip)}</title>"
    if shape == "circle":
        return f'<circle class="mark {cls}" cx="{x:.1f}" cy="{y:.1f}" r="4.5">{t}</circle>'
    if shape == "square":
        return f'<rect class="mark {cls}" x="{x - 4.5:.1f}" y="{y - 4.5:.1f}" width="9" height="9" rx="1.5">{t}</rect>'
    return (f'<path class="mark {cls}" d="M{x:.1f},{y - 5.5:.1f} L{x + 5.5:.1f},{y + 4.5:.1f} '
            f'L{x - 5.5:.1f},{y + 4.5:.1f} Z">{t}</path>')


def panel(px, py, pw, ph, spec):
    """One plot area. spec: title, xlabel, ylabel, xlog, ylog, series{key:[(x,y)]}, ref, yfmt, tipfmt."""
    out = []
    pts = [p for s in spec["series"].values() for p in s]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    xlo, xhi = spec.get("xdomain") or (min(xs), max(xs))
    ylo, yhi = spec.get("ydomain") or (min(ys), max(ys))
    if xlo == xhi:                                 # a single point still needs a visible axis
        xlo, xhi = (xlo / 2, xhi * 2) if spec["xlog"] else (xlo - 1, xhi + 1)
    if yhi <= 0:
        yhi = 1
    if spec["ylog"]:
        ylo, yhi = 10 ** math.floor(math.log10(max(ylo, 1e-9))), 10 ** math.ceil(math.log10(yhi))
    else:
        ylo, yhi = 0, yhi * 1.1
    left, right, top, bottom = 68, 128, 28, 40
    x0, x1, y0, y1 = px + left, px + pw - right, py + top, py + ph - bottom

    def X(v):
        if spec["xlog"]:
            return x0 + (math.log10(v) - math.log10(xlo)) / (math.log10(xhi) - math.log10(xlo)) * (x1 - x0)
        return x0 + (v - xlo) / (xhi - xlo) * (x1 - x0)

    def Y(v):
        if spec["ylog"]:
            v = max(v, ylo)
            return y1 - (math.log10(v) - math.log10(ylo)) / (math.log10(yhi) - math.log10(ylo)) * (y1 - y0)
        return y1 - (v - ylo) / (yhi - ylo) * (y1 - y0)

    out.append(f'<text class="ptitle" x="{x0}" y="{py + 14}">{esc(spec["title"])}</text>')
    yticks = nice_log_ticks(ylo, yhi) if spec["ylog"] else nice_lin_ticks(ylo, yhi)
    if spec["ylog"] and len(yticks) > 7:
        yticks = [t for t in yticks if str(t).lstrip("0.").startswith("1")]
    yf = spec.get("yfmt", fmt)
    for t in yticks:
        out.append(f'<line class="grid" x1="{x0}" x2="{x1}" y1="{Y(t):.1f}" y2="{Y(t):.1f}"/>')
        out.append(f'<text class="tick" x="{x0 - 6}" y="{Y(t) + 3.5:.1f}" text-anchor="end">{yf(t)}</text>')
    xticks = spec.get("xticks") or sorted({p[0] for p in pts})
    for t in xticks:
        out.append(f'<text class="tick" x="{X(t):.1f}" y="{y1 + 15}" text-anchor="middle">{fmt(t)}</text>')
    out.append(f'<line class="axis" x1="{x0}" x2="{x1}" y1="{y1}" y2="{y1}"/>')
    out.append(f'<text class="alabel" x="{(x0 + x1) / 2:.1f}" y="{y1 + 32}" text-anchor="middle">{esc(spec["xlabel"])}</text>')
    out.append(f'<text class="alabel" transform="translate({px + 12},{(y0 + y1) / 2:.1f}) rotate(-90)" '
               f'text-anchor="middle">{esc(spec["ylabel"])}</text>')
    if spec.get("ref") == "diagonal":
        a, b = max(xlo, ylo), min(xhi, yhi)
        out.append(f'<line class="ref" x1="{X(a):.1f}" y1="{Y(a):.1f}" x2="{X(b):.1f}" y2="{Y(b):.1f}"/>')
        out.append(f'<text class="note" x="{X(b) - 4:.1f}" y="{Y(b) + 14:.1f}" text-anchor="end">offered = achieved</text>')
    if spec.get("ref_y") is not None:
        ry, label = spec["ref_y"]
        out.append(f'<line class="ref" x1="{x0}" x2="{x1}" y1="{Y(ry):.1f}" y2="{Y(ry):.1f}"/>')
        out.append(f'<text class="note" x="{x0 + 4}" y="{Y(ry) - 5:.1f}">{esc(label)}</text>')

    ends = []
    for slot, (key, name, shape) in enumerate(APPROACHES, start=1):
        s = sorted(spec["series"].get(key, []))
        if not s:
            continue
        cls = f"s{slot}"
        d = " ".join(f"{'M' if i == 0 else 'L'}{X(x):.1f},{Y(y):.1f}" for i, (x, y) in enumerate(s))
        out.append(f'<path class="line {cls}" d="{d}"/>')
        tipf = spec.get("tipfmt", lambda x, y: f"{fmt(x)}: {y:g}")
        for x, y in s:
            out.append(marker(shape, X(x), Y(y), cls, f"{name} · {tipf(x, y)}"))
        ends.append([Y(s[-1][1]), name, cls, X(s[-1][0])])
    ends.sort()                                    # direct labels at line ends, nudged apart
    for i in range(1, len(ends)):
        ends[i][0] = max(ends[i][0], ends[i - 1][0] + 15)
    for y, name, cls, xe in ends:
        out.append(f'<line class="swatch {cls}" x1="{x1 + 10}" x2="{x1 + 22}" y1="{y:.1f}" y2="{y:.1f}"/>')
        out.append(f'<text class="dlabel" x="{x1 + 26}" y="{y + 4:.1f}">{esc(name)}</text>')
    return out


def figure(path, title, subtitle, panels, cols=2, pw=470, ph=300, note=None):
    rows = math.ceil(len(panels) / cols)
    width, head, legend_h = cols * pw, 64, 26
    height = head + legend_h + rows * ph + (22 if note else 6)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
           f'role="img" aria-label="{esc(title)}">', STYLE, '<g class="viz">',
           f'<rect class="bg" width="{width}" height="{height}" rx="6"/>',
           f'<text class="title" x="16" y="26">{esc(title)}</text>',
           f'<text class="sub" x="16" y="46">{esc(subtitle)}</text>']
    lx = 16                                        # legend row (always present: 3 series)
    for slot, (_, name, shape) in enumerate(APPROACHES, start=1):
        out.append(f'<line class="swatch line s{slot}" x1="{lx}" x2="{lx + 22}" y1="{head + 6}" y2="{head + 6}"/>')
        out.append(marker(shape, lx + 11, head + 6, f"s{slot}", name))
        out.append(f'<text class="dlabel" x="{lx + 28}" y="{head + 10}">{esc(name)}</text>')
        lx += 40 + len(name) * 7
    for i, spec in enumerate(panels):
        r, c = divmod(i, cols)
        out += panel(c * pw, head + legend_h + r * ph, pw, ph, spec)
    if note:
        out.append(f'<text class="note" x="16" y="{height - 10}">{esc(note)}</text>')
    out.append("</g></svg>")
    Path(path).write_text("\n".join(out))
    print("wrote", path)


def main():
    src, dest = Path(sys.argv[1]), Path(sys.argv[2])
    dest.mkdir(parents=True, exist_ok=True)
    rows = list(csv.DictReader(src.open()))
    num = lambda r, k: float(r[k])
    metrics = ("achieved_rps", "goodput_rps", "p50_ms", "p99_ms", "db_tx_per_confirmed", "peak_db_in_flight")
    trials = defaultdict(list)                       # (scenario, approach, rate, metric) -> values per trial
    for r in rows:
        if r["scenario"] in ("normal", "hot"):
            for m in metrics:
                trials[(r["scenario"], r["approach"], num(r, "offered_rps"), m)].append(num(r, m))
            correct = num(r, "confirmed") + num(r, "no_capacity")     # errors are not answers
            share = correct / num(r, "answered") if num(r, "answered") else 0
            trials[(r["scenario"], r["approach"], num(r, "offered_rps"), "correct_rps")].append(num(r, "achieved_rps") * share)
    steady = defaultdict(lambda: defaultdict(list))   # (scenario, metric) -> approach -> [(x, median y)]
    for (scn, appr, x, m), vals in sorted(trials.items()):
        steady[(scn, m)][appr].append((x, statistics.median(vals)))
    burst = defaultdict(lambda: defaultdict(list))
    for r in rows:
        if r["scenario"] == "burst":
            burst[(r["approach"], int(r["burst_size"]))]["max"].append(num(r, "max_ms"))
            burst[(r["approach"], int(r["burst_size"]))]["failed"].append(
                100 * (num(r, "failed") + num(r, "unfinished")) / num(r, "requests"))
    rates = {s: sorted({x for xs in steady[(s, "p99_ms")].values() for x, _ in xs}) for s in ("normal", "hot")}
    ms = lambda v: f"{v:g} ms" if v < 1000 else f"{v / 1000:g} s"

    def lat_panel(scn, metric, title):
        return dict(title=title, xlabel="offered load (requests/s)", ylabel=f"{metric[:3]} latency (log)",
                    xlog=True, ylog=True, series=steady[(scn, metric)], xticks=rates[scn],
                    yfmt=lambda v: ms(v), tipfmt=lambda x, y: f"{fmt(x)} req/s → {ms(round(y, 1))}",
                    ref_y=(500, "500 ms SLO"))

    figure(dest / "latency.svg", "Time until the client knows the outcome",
           "p50 and p99 from scheduled arrival to confirmed/rejected; open-loop Poisson load, median of 3 trials",
           [lat_panel("normal", "p50_ms", "Normal day: p50"), lat_panel("hot", "p50_ms", "Hot dealership: p50"),
            lat_panel("normal", "p99_ms", "Normal day: p99"), lat_panel("hot", "p99_ms", "Hot dealership: p99")],
           note="O2 latency includes a simulated 5 ms broker hop. Lower is better.")

    def thr_panel(scn, metric, title, ylabel):
        return dict(title=title, xlabel="offered load (requests/s)", ylabel=ylabel, xlog=True, ylog=True,
                    series=steady[(scn, metric)], xticks=rates[scn], ref="diagonal",
                    tipfmt=lambda x, y: f"{fmt(x)} offered → {y:.0f}/s")

    figure(dest / "throughput.svg", "Throughput: how much load each approach absorbs",
           "Correct answer = booked or no capacity; errors excluded. Top: any latency. Bottom: within 500 ms. Median of 3 trials",
           [thr_panel("normal", "correct_rps", "Normal day: correct answers / s", "correct answers per second (log)"),
            thr_panel("hot", "correct_rps", "Hot dealership: correct answers / s", "correct answers per second (log)"),
            thr_panel("normal", "goodput_rps", "Normal day: answered within 500 ms / s", "within SLO per second (log)"),
            thr_panel("hot", "goodput_rps", "Hot dealership: answered within 500 ms / s", "within SLO per second (log)")])

    def work_panel(scn, metric, title, ylabel, ylog):
        return dict(title=title, xlabel="offered load (requests/s)", ylabel=ylabel, xlog=True, ylog=ylog,
                    series=steady[(scn, metric)], xticks=rates[scn], yfmt=lambda v: f"{v:g}",
                    tipfmt=lambda x, y: f"{fmt(x)} req/s → {y:g}")

    figure(dest / "db-cost.svg", "What each approach costs the database",
           "Transactions per confirmed booking (retries and batching) and the peak number of connections in use",
           [work_panel("normal", "db_tx_per_confirmed", "Normal day: transactions / booking", "tx per booking (log)", True),
            work_panel("hot", "db_tx_per_confirmed", "Hot dealership: transactions / booking", "tx per booking (log)", True),
            work_panel("normal", "peak_db_in_flight", "Normal day: peak connections in use", "connections (pool max 50)", False),
            work_panel("hot", "peak_db_in_flight", "Hot dealership: peak connections in use", "connections (pool max 50)", False)])

    sizes = sorted({k[1] for k in burst})
    bmax = {a: [(n, statistics.median(burst[(a, n)]["max"])) for n in sizes if (a, n) in burst] for a, _, _ in APPROACHES}
    bfail = {a: [(n, statistics.mean(burst[(a, n)]["failed"])) for n in sizes if (a, n) in burst] for a, _, _ in APPROACHES}
    figure(dest / "burst.svg", "Burst: N customers book the same slot at the same instant",
           "One dealership, 6 bays: 6 bookings fit and the rest must hear 'no capacity'. Median of 3 trials",
           [dict(title="Time until the last customer gets an answer", xlabel="simultaneous requests (N)",
                 ylabel="slowest answer (log)", xlog=True, ylog=True, series=bmax, xticks=sizes,
                 yfmt=lambda v: ms(v), tipfmt=lambda x, y: f"N={x} → {ms(round(y))}"),
            dict(title="Requests that got an error instead of an answer", xlabel="simultaneous requests (N)",
                 ylabel="errors (% of requests)", xlog=True, ylog=False, series=bfail, xticks=sizes,
                 ydomain=(0, 100), yfmt=lambda v: f"{v:g}%", tipfmt=lambda x, y: f"N={x} → {y:.0f}% errors")])


def tables(src):
    """Median-of-trials tables in markdown: the data behind every chart."""
    rows = list(csv.DictReader(Path(src).open()))
    names = {k: n for k, n, _ in APPROACHES}
    groups = defaultdict(list)
    for r in rows:
        groups[(r["scenario"], r["approach"], float(r["offered_rps"]), int(r["burst_size"]))].append(r)
    med = lambda rs, k: statistics.median(float(r[k]) for r in rs)
    order = {k: i for i, (k, _, _) in enumerate(APPROACHES)}
    for scn in ("normal", "hot"):
        print(f"\n**{scn}**\n")
        print("| Offered req/s | Approach | Goodput req/s | p50 | p99 | Errors | Tx / booking | Peak connections |")
        print("|---:|---|---:|---:|---:|---:|---:|---:|")
        for (s, a, rate, _), rs in sorted(groups.items(), key=lambda kv: (kv[0][2], order[kv[0][1]])):
            if s != scn:
                continue
            errs = med(rs, "failed") + med(rs, "unfinished")
            print(f"| {rate:g} | {names[a]} | {med(rs, 'goodput_rps'):.0f} | {med(rs, 'p50_ms'):.1f} ms | "
                  f"{med(rs, 'p99_ms'):.1f} ms | {errs:g} | {med(rs, 'db_tx_per_confirmed'):.2f} | "
                  f"{med(rs, 'peak_db_in_flight'):g} |")
    print("\n**burst**\n")
    print("| N at once | Approach | Booked | 'No capacity' | Errors | Deadlocks hit | Slowest answer |")
    print("|---:|---|---:|---:|---:|---:|---:|")
    for (s, a, _, n), rs in sorted(groups.items(), key=lambda kv: (kv[0][3], order[kv[0][1]])):
        if s == "burst":
            print(f"| {n} | {names[a]} | {med(rs, 'confirmed'):g} | {med(rs, 'no_capacity'):g} | "
                  f"{med(rs, 'failed') + med(rs, 'unfinished'):g} | {med(rs, 'deadlock_retries'):g} | "
                  f"{med(rs, 'max_ms'):.0f} ms |")


if __name__ == "__main__":
    if sys.argv[1] == "--tables":
        tables(sys.argv[2])
    else:
        main()
