#!/usr/bin/env python3
"""Summary of the partial E13ack (intra-AZ, metronome ACK-arm) run that crashed.
Throughput + p99 latency vs concurrency, faceted by value size, one line per N.
Aggregates the multiple runs per cell by median."""
import sys
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

df = pd.read_csv("/tmp/e13/E13-ack-per-cell-parsed.csv")
df = df[df.tps > 0]
# median across runs per (N, value_size, c)
g = (df.groupby(["n_servers", "value_size", "c"])
       .agg(tps=("tps", "median"), p99=("p99_lat_ms", "median"), runs=("run", "count"))
       .reset_index())

vals = sorted(g.value_size.unique())
Ns = sorted(g.n_servers.unique())
colors = {3: "#1f77b4", 5: "#ff7f0e", 7: "#2ca02c"}

fig, axes = plt.subplots(2, len(vals), figsize=(5 * len(vals), 8), squeeze=False)
for j, v in enumerate(vals):
    sub = g[g.value_size == v]
    ax_t, ax_l = axes[0][j], axes[1][j]
    for n in Ns:
        s = sub[sub.n_servers == n].sort_values("c")
        if s.empty:
            continue
        ax_t.plot(s.c, s.tps / 1000, "o-", color=colors[n], label=f"N={n}")
        ax_l.plot(s.c, s.p99, "o-", color=colors[n], label=f"N={n}")
    ax_t.set_title(f"value={v//1024}kB")
    ax_t.set_ylabel("throughput (k ops/s)")
    ax_t.grid(alpha=.3); ax_t.legend()
    ax_l.set_xlabel("concurrency (clients)")
    ax_l.set_ylabel("p99 latency (ms)")
    ax_l.grid(alpha=.3); ax_l.legend()

fig.suptitle("E13ack (intra-AZ, metronome ACK-arm) — PARTIAL run before disk-full crash\n"
             f"cells captured: {len(g)}  |  value sizes: {[v//1024 for v in vals]}kB", y=1.00)
fig.tight_layout()
out = "/tmp/e13/E13ack-partial-summary.png"
fig.savefig(out, dpi=110, bbox_inches="tight")
print("wrote", out)

# text summary
print("\n=== cells captured per (value_size, N) ===")
piv = (g.assign(vkb=g.value_size // 1024)
         .pivot_table(index="vkb", columns="n_servers", values="c", aggfunc="count", fill_value=0))
print(piv.to_string())
print("\n=== peak throughput (k ops/s) per (value_size, N) ===")
pk = (g.assign(vkb=g.value_size // 1024).groupby(["vkb", "n_servers"]).tps.max() / 1000).round(1)
print(pk.to_string())
