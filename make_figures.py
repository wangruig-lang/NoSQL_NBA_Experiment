"""Summarize the failover trials and draw the latency and timeline figures.

Usage:  python make_figures.py
Reads   results/e3_trials.csv, results/e3_attempts.csv, results/e1_summary.csv
Writes  results/e3_summary.csv, results/fig_e1_latency.png (+ .pdf),
        results/fig_failover_timeline.png (+ .pdf)
"""
import glob
import math

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter
import pandas as pd

from lab import RESULTS

# Two hues carry the guarantee (validated categorical slots 1-2); line style + marker carry the
# database, so the figure still reads in grayscale print.
STRONG, WEAK = "#2a78d6", "#eb6834"
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
SERIES = {  # mode: (label, color, linestyle, marker)
    "mongo-majority": ("MongoDB majority",   STRONG, "-", "o"),
    "redis-wait":     ("Redis WAIT",         STRONG, "--", "s"),
    "mongo-w1":       ("MongoDB w:1",        WEAK,   "-", "o"),
    "redis-async":    ("Redis async",        WEAK,   "--", "s"),
}
MODE_ORDER = list(SERIES)


def e3_table():
    df = pd.read_csv(RESULTS / "e3_trials.csv")
    g = df.groupby(["mode", "rate"])
    out = pd.DataFrame({
        "trials": g.size(),
        "sent_in_partition": g.window_attempts.mean().round(1),
        "acked_in_partition": g.window_acked.mean().round(1),
        "lost_mean": g.lost.mean().round(1),
        "lost_min": g.lost.min(),
        "lost_max": g.lost.max(),
        "rolled_back_mean": g.rolled_back.mean().round(1) if df.rolled_back.notna().any() else None,
        "unavailable_s_median": g.unavailable_s.median().round(1),
        "final_count_min": g.final_count.min(),
    }).reset_index()
    out["mode"] = pd.Categorical(out["mode"], MODE_ORDER, ordered=True)
    out = out.sort_values(["mode", "rate"])
    out.to_csv(RESULTS / "e3_summary.csv", index=False)
    print(out.to_string(index=False))


def e1_figure():
    df = pd.read_csv(RESULTS / "e1_summary.csv")
    delays = sorted(df.delay_ms.unique())
    x = {d: i for i, d in enumerate(delays)}  # evenly spaced categories: 0 ms can't sit on a log axis

    # IEEE template: figure labels in 8 pt Times New Roman (fonts/ holds the .ttf files if the system lacks them)
    for f in glob.glob("fonts/*.ttf"):
        font_manager.fontManager.addfont(f)
    have_tnr = any(f.name == "Times New Roman" for f in font_manager.fontManager.ttflist)
    plt.rcParams.update({"font.size": 8, "font.family": "Times New Roman" if have_tnr else "STIXGeneral",
                         "legend.fontsize": 8, "axes.edgecolor": MUTED,
                         "axes.labelcolor": INK, "xtick.color": MUTED, "ytick.color": MUTED})
    fig, ax = plt.subplots(figsize=(3.5, 2.6), dpi=300)
    ends = []
    for mode in MODE_ORDER:
        label, color, ls, marker = SERIES[mode]
        s = df[df["mode"] == mode].sort_values("delay_ms")
        xs = [x[d] for d in s.delay_ms]
        ax.plot(xs, s.p50_ms, ls=ls, lw=1.4, color=color, marker=marker, ms=4.5,
                mec="white", mew=0.8, label=label, zorder=3)
        ax.vlines(xs, s.p50_ms, s.p95_ms, color=color, lw=0.8, alpha=0.6, zorder=2)  # whisker to p95
        ends.append([math.log10(s.p50_ms.iloc[-1]), label])

    # direct labels at the right end, nudged apart so they never overlap
    ends.sort()
    for k in range(1, len(ends)):
        ends[k][0] = max(ends[k][0], ends[k - 1][0] + 0.2)
    for y, label in ends:
        ax.text(len(delays) - 1 + 0.12, 10 ** y, label, va="center", fontsize=8, color=INK)

    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))  # 0.1, 1, 10, 100 instead of 10^n
    ax.set_xticks(range(len(delays)), [f"{d}" for d in delays])
    ax.set_xlim(-0.3, len(delays) - 1 + 1.6)
    ax.set_xlabel("Added replica egress delay (ms)")
    ax.set_ylabel("Write latency (ms)")
    ax.grid(axis="y", which="major", color=GRID, lw=0.6)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(loc="upper left", fontsize=8, frameon=False, handlelength=2.4)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(RESULTS / f"fig_e1_latency.{ext}", facecolor="white")
    print(f"wrote {RESULTS / 'fig_e1_latency.png'}")


def timeline_figure(rate=1.0):
    """Fig.: one failover trial per mode at the given rate, on a shared time axis from disconnection."""
    trials = pd.read_csv(RESULTS / "e3_trials.csv")
    attempts = pd.read_csv(RESULTS / "e3_attempts.csv")
    rows = [("mongo-w1", "MongoDB w:1"), ("mongo-majority", "MongoDB majority"),
            ("redis-async", "Redis async"), ("redis-wait", "Redis WAIT")]
    plt.rcParams.update({"font.size": 8, "font.family": "Times New Roman" if _tnr() else "STIXGeneral"})
    fig, ax = plt.subplots(figsize=(3.5, 2.05), dpi=300)
    for y, (mode, label) in enumerate(reversed(rows)):
        t = trials[(trials["mode"] == mode) & (trials["rate"] == rate)].iloc[0]
        a = attempts[(attempts["mode"] == mode) & (attempts["rate"] == rate)]
        a = a[a["trial_start"] == a["trial_start"].iloc[0]]
        weak = mode in ("mongo-w1", "redis-async")
        for _, r in a.iterrows():
            ax.plot([r.start_s, max(r.end_s, r.start_s + 0.08)], [y, y], lw=5, solid_capstyle="butt",
                    color=WEAK if r.acked else STRONG, zorder=3)
        ax.plot([t.kill_s], [y], marker="x", ms=6, mew=1.4, color=INK, zorder=4)
        resumed = t.kill_s + t.unavailable_s
        ax.plot([t.kill_s, resumed], [y, y], lw=1, ls=":", color=MUTED, zorder=2)
        ax.plot([resumed], [y], marker="|", ms=7, mew=1.4, color=INK, zorder=4)
        lost = int(t.lost)
        ax.text(resumed + 0.3, y, f"lost {lost}", va="center", fontsize=7,
                color=INK if lost else MUTED)
    ax.axvspan(0, 8, color=GRID, alpha=0.6, zorder=0, lw=0)
    ax.text(4, -0.55, "replicas disconnected", ha="center", va="center", fontsize=7, color=MUTED)
    ax.set_yticks(range(len(rows)), [lbl for _, lbl in reversed(rows)])
    ax.set_ylim(-0.8, len(rows) - 0.4)
    ax.set_xlim(-0.3, 16.5)
    ax.set_xlabel("Time since replica disconnection (s)")
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="y", length=0)
    from matplotlib.lines import Line2D
    ax.legend(handles=[Line2D([], [], color=WEAK, lw=5, label="acked"),
                       Line2D([], [], color=STRONG, lw=5, label="rejected"),
                       Line2D([], [], marker="x", color=INK, lw=0, label="primary killed"),
                       Line2D([], [], marker="|", color=INK, lw=0, ms=7, label="writes resume")],
              loc="lower center", bbox_to_anchor=(0.45, 1.0), ncol=2, fontsize=7, frameon=False,
              handlelength=1.4, columnspacing=1.2)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(RESULTS / f"fig_failover_timeline.{ext}", facecolor="white")
    print(f"wrote {RESULTS / 'fig_failover_timeline.png'}")


def _tnr():
    for f in glob.glob("fonts/*.ttf"):
        font_manager.fontManager.addfont(f)
    return any(f.name == "Times New Roman" for f in font_manager.fontManager.ttflist)


if __name__ == "__main__":
    if (RESULTS / "e3_trials.csv").exists():
        e3_table()
    if (RESULTS / "e1_summary.csv").exists():
        e1_figure()
    if (RESULTS / "e3_attempts.csv").exists():
        timeline_figure()
