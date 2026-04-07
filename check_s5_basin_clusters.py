#!/usr/bin/env python3
"""
s5 流域合并结果抽查脚本

默认在「多数据源」cluster 中，按数据源种类数 n_sources 从高到低取前若干个，
在地图上展示站点；每个 source 使用不同颜色与不同 marker，子图旁标注含哪些数据集。

输出（离线友好，不访问网络）：
  check_output/s5_cluster_check.png   — 静态多子图：basemap 或纯经纬度

运行方式（计算节点离线请加 --plain-only）：
  python check_s5_basin_clusters.py
  python check_s5_basin_clusters.py --n 12 --plain-only
  python check_s5_basin_clusters.py --random-sample --seed 99  # 改为随机抽查
  python check_s5_basin_clusters.py --no-multi-source-filter
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # 无显示器环境
import matplotlib.pyplot as plt

from pipeline_paths import S5_BASIN_CLUSTERED_CSV, get_output_r_root

SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
DEFAULT_CSV  = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUT  = SCRIPT_DIR / "check_output"

# 各数据源的颜色（循环使用）
_SOURCE_COLORS = [
    "#e41a1c", "#377eb8", "#4daf4a", "#984ea3",
    "#ff7f00", "#a65628", "#f781bf", "#999999",
    "#1b9e77", "#d95f02", "#7570b3",
]

# 各数据源对应不同 marker（与颜色独立循环）
_SOURCE_MARKERS = [
    "o", "s", "^", "D", "v", "P", "X", "<", ">", "p", "*", "h", "8",
]


def _source_style_map(sources):
    """每个 source 分配 (color, marker)，形状、颜色均在图例中区分。"""
    unique = sorted(set(sources))
    return {
        s: (
            _SOURCE_COLORS[i % len(_SOURCE_COLORS)],
            _SOURCE_MARKERS[i % len(_SOURCE_MARKERS)],
        )
        for i, s in enumerate(unique)
    }


def _bbox_with_buffer(lats, lons, min_buf=0.3):
    """计算含缓冲区的经纬度范围，返回 (lat_min, lat_max, lon_min, lon_max)。"""
    spread = max(max(lats) - min(lats), max(lons) - min(lons))
    buf    = max(min_buf, spread * 0.6)
    return (
        max(-89.9, min(lats) - buf),
        min( 89.9, max(lats) + buf),
        max(-179.9, min(lons) - buf),
        min( 179.9, max(lons) + buf),
    )


def _cluster_panel_title(cluster_df, cid):
    """子图标题（与底图库无关）。"""
    n_mem      = len(cluster_df)
    n_src      = cluster_df["source"].nunique()
    basin_id   = cluster_df["basin_id"].iloc[0] if "basin_id" in cluster_df.columns else "N/A"
    basin_area = cluster_df["basin_area"].iloc[0] if "basin_area" in cluster_df.columns else np.nan
    pfaf       = cluster_df["pfaf_code"].iloc[0] if "pfaf_code" in cluster_df.columns else np.nan
    mq         = cluster_df["match_quality"].iloc[0] if "match_quality" in cluster_df.columns else "N/A"
    area_str   = "{:.0f} km²".format(basin_area) if pd.notna(basin_area) else "N/A"
    pfaf_str   = str(int(pfaf)) if pd.notna(pfaf) else "N/A"
    return (
        "CID={cid}  n={n}  n_src={ns}\nbasin={bid}  area={area}\npfaf={pfaf}  mq={mq}".format(
            cid=cid, n=n_mem, ns=n_src,
            bid=str(basin_id)[:12] if pd.notna(basin_id) else "N/A",
            area=area_str, pfaf=pfaf_str, mq=mq,
        )
    )


def _cluster_sources_sidebar_lines(cluster_df):
    """子图右侧说明：本 cluster 包含的数据集列表（每行一个）。"""
    names = sorted(cluster_df["source"].astype(str).unique())
    lines = ["datasets ({})".format(len(names))] + ["• {}".format(nm) for nm in names]
    return "\n".join(lines)


def _draw_cluster_stations_mpl(ax, cluster_df, style_map, x_of_lonlat):
    """按 source 使用不同颜色与 marker；代表站略大、黑边加粗。"""
    for _, row in cluster_df.iterrows():
        x, y = x_of_lonlat(row["lon"], row["lat"])
        is_rep = (row["station_id"] == row["cluster_id"])
        src    = row["source"]
        color, marker = style_map.get(src, ("#888888", "o"))
        ms  = 11 if is_rep else 7
        mew = 1.35 if is_rep else 0.55
        mec = "black" if is_rep else "white"
        ax.plot(
            x, y,
            marker=marker,
            markersize=ms,
            markerfacecolor=color,
            markeredgecolor=mec,
            markeredgewidth=mew,
            linestyle="",
            zorder=6 if is_rep else 4,
            alpha=0.92,
        )


def _annotate_sources_beside_map(ax, cluster_df):
    """在地图坐标轴右侧用文字框列出本 cluster 的数据集（不裁剪）。"""
    txt = _cluster_sources_sidebar_lines(cluster_df)
    ax.text(
        1.02,
        0.5,
        txt,
        transform=ax.transAxes,
        va="center",
        ha="left",
        fontsize=5.8,
        linespacing=1.15,
        bbox=dict(boxstyle="round,pad=0.25", facecolor="#fffef0", edgecolor="#ccccaa", alpha=0.92),
        clip_on=False,
        zorder=20,
    )


def _select_static_map_backend():
    """静态图底图：basemap（仅用包内本地数据）> 纯 matplotlib。不使用 cartopy（会在线拉地理数据）。"""
    try:
        from mpl_toolkits.basemap import Basemap  # noqa: F401, E402
        return "basemap"
    except ImportError:
        pass
    return "plain"


def _plot_one_cluster_basemap(ax, cluster_df, style_map, cid):
    """使用 Basemap 绘制单个子图。"""
    from mpl_toolkits.basemap import Basemap  # noqa: E402

    lats = cluster_df["lat"].values
    lons = cluster_df["lon"].values
    lat_min, lat_max, lon_min, lon_max = _bbox_with_buffer(lats, lons)

    bm = Basemap(
        projection="merc",
        llcrnrlat=lat_min, urcrnrlat=lat_max,
        llcrnrlon=lon_min, urcrnrlon=lon_max,
        resolution="l",
        ax=ax,
    )
    bm.drawcoastlines(linewidth=0.5, color="gray")
    bm.drawcountries(linewidth=0.4, color="lightgray")
    bm.fillcontinents(color="#f5f5f5", lake_color="#d0e8ff", alpha=0.6)
    bm.drawmapboundary(fill_color="#d0e8ff")
    try:
        bm.drawrivers(linewidth=0.3, color="#6baed6")
    except Exception:
        pass

    _draw_cluster_stations_mpl(ax, cluster_df, style_map, lambda lo, la: bm(lo, la))
    ax.set_title(_cluster_panel_title(cluster_df, cid), fontsize=7, pad=3)
    _annotate_sources_beside_map(ax, cluster_df)


def _plot_one_cluster_plain(ax, cluster_df, style_map, cid):
    """纯 matplotlib：经纬度散点 + 网格（无海岸线，完全离线）。"""
    lats = cluster_df["lat"].values
    lons = cluster_df["lon"].values
    lat_min, lat_max, lon_min, lon_max = _bbox_with_buffer(lats, lons)
    ax.set_xlim(lon_min, lon_max)
    ax.set_ylim(lat_min, lat_max)
    mid_lat = float(np.clip(np.mean(lats), -60.0, 60.0))
    ax.set_aspect(1.0 / max(np.cos(np.radians(mid_lat)), 0.2))

    _draw_cluster_stations_mpl(ax, cluster_df, style_map, lambda lo, la: (lo, la))
    ax.set_xlabel("lon", fontsize=6)
    ax.set_ylabel("lat", fontsize=6)
    ax.grid(True, alpha=0.35, linewidth=0.5)
    ax.set_facecolor("#e8f4fc")
    ax.set_title(_cluster_panel_title(cluster_df, cid), fontsize=7, pad=3)
    _annotate_sources_beside_map(ax, cluster_df)


def plot_static(sampled_clusters, stations_df, out_png, plain_only=False, random_sample=False):
    """生成静态多子图 PNG（每个 cluster 一个子图）；不依赖网络。"""
    backend = "plain" if plain_only else _select_static_map_backend()
    if backend == "basemap":
        print("  Static map backend: basemap (local shape data)")
    else:
        print("  Static map backend: plain lat/lon (no basemap)")

    n = len(sampled_clusters)
    ncols = min(4, n)
    nrows = (n + ncols - 1) // ncols
    # 右侧留空给数据集文字框
    figsize = (ncols * 5.4, nrows * 4.0)

    all_sources = stations_df[stations_df["cluster_id"].isin(sampled_clusters)]["source"].tolist()
    style_map   = _source_style_map(all_sources)

    fig, axes = plt.subplots(nrows, ncols, figsize=figsize, constrained_layout=True)
    if n == 1:
        axes = np.array([[axes]])
    elif nrows == 1:
        axes = axes.reshape(1, -1)

    plot_fn = _plot_one_cluster_basemap if backend == "basemap" else _plot_one_cluster_plain
    for plot_i, cid in enumerate(sampled_clusters):
        row_i = plot_i // ncols
        col_i = plot_i % ncols
        ax    = axes[row_i, col_i]
        cluster_df = stations_df[stations_df["cluster_id"] == cid].copy()
        try:
            plot_fn(ax, cluster_df, style_map, cid)
        except Exception as e:
            ax.set_title("CID={} ERROR\n{}".format(cid, str(e)[:60]), fontsize=7)
            ax.axis("off")

    for plot_i in range(n, nrows * ncols):
        axes[plot_i // ncols, plot_i % ncols].axis("off")

    # 图例：每个 source 用对应颜色 + marker
    legend_handles = []
    for src in sorted(style_map.keys()):
        col, mk = style_map[src]
        legend_handles.append(
            plt.Line2D(
                [0], [0],
                marker=mk,
                color="none",
                markerfacecolor=col,
                markeredgecolor="black",
                markeredgewidth=0.4,
                markersize=9,
                linestyle="",
                label=src,
            )
        )
    legend_handles.append(
        plt.Line2D(
            [0], [0],
            marker="o",
            color="gray",
            markersize=8,
            linestyle="",
            markeredgecolor="black",
            markeredgewidth=1.2,
            label="Representative (cluster_id==station_id): larger + black edge",
        )
    )
    fig.legend(
        handles=legend_handles,
        loc="lower center",
        ncol=min(4, max(1, len(legend_handles))),
        fontsize=7,
        frameon=True,
        bbox_to_anchor=(0.5, -0.04),
    )

    if random_sample:
        st = (
            "s5 Basin Cluster Check — random sample; marker shape = source; "
            "sidebar = dataset list"
        )
    else:
        st = (
            "s5 Basin Cluster Check — top clusters by #datasets; marker shape = source; "
            "sidebar = dataset list"
        )
    fig.suptitle(st, fontsize=10.5, y=1.05)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("Static map saved → {}".format(out_png))


def main():
    ap = argparse.ArgumentParser(
        description="s5 流域合并结果抽查：默认取 n_sources 最多的若干 cluster 作图（可加 --random-sample 随机）"
    )
    ap.add_argument("--csv", default=str(DEFAULT_CSV),
                    help="s5_basin_clustered_stations.csv 路径。默认: {}".format(DEFAULT_CSV))
    ap.add_argument("--n",   type=int, default=9,
                    help="作图 cluster 数量（默认 9，建议 4–16）")
    ap.add_argument("--seed", type=int, default=42,
                    help="仅 --random-sample 时生效：随机种子")
    ap.add_argument("--random-sample", action="store_true",
                    help="从候选 cluster 中随机抽 --n 个（默认改为按 n_sources 从高到低取前 --n 个）")
    ap.add_argument("--min-stations", type=int, default=2,
                    help="额外要求站点数 >= 该值（默认 2；多源模式下通常已隐含至少 2 条记录）")
    ap.add_argument("--min-sources", type=int, default=2,
                    help="多源模式下要求 cluster 内不同 source 种数 >= 该值（默认 2）")
    ap.add_argument("--no-multi-source-filter", action="store_true",
                    help="不按数据源种类筛选，改为仅按 --min-stations / --include-single（与旧版一致）")
    ap.add_argument("--include-single", action="store_true",
                    help="与 --no-multi-source-filter 联用：允许 n_members==1 的 cluster 进入候选")
    ap.add_argument("--out", default=str(DEFAULT_OUT),
                    help="输出目录（默认: check_output/）")
    ap.add_argument("--no-static", action="store_true", help="跳过静态 PNG 生成")
    ap.add_argument("--plain-only", action="store_true",
                    help="强制使用纯经纬度子图，不尝试 basemap（离线环境推荐）")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    out_dir  = Path(args.out)

    if not csv_path.is_file():
        print("Error: CSV not found: {}".format(csv_path))
        return 1

    # ── 读取数据 ──────────────────────────────────────────────────────────
    print("Reading {}".format(csv_path))
    df = pd.read_csv(csv_path)
    print("  {} rows, {} unique clusters".format(len(df), df["cluster_id"].nunique()))

    # ── 统计每个 cluster：站点数、数据源种数 ─────────────────────────────
    counts = df.groupby("cluster_id")["station_id"].count().rename("n_members")
    n_sources = df.groupby("cluster_id")["source"].nunique().rename("n_sources")
    clust_stats = pd.concat([counts, n_sources], axis=1)
    df = df.merge(clust_stats, on="cluster_id", how="left")

    # ── 按条件筛选候选 cluster ────────────────────────────────────────────
    if args.no_multi_source_filter:
        min_n = 1 if args.include_single else args.min_stations
        candidates = clust_stats[clust_stats["n_members"] >= min_n].index.tolist()
        print("  候选 cluster（仅按 n_members >= {}）: {}".format(min_n, len(candidates)))
        if not candidates:
            print("Error: 没有符合条件的 cluster，请降低 --min-stations 或加 --include-single")
            return 1
    else:
        ms = max(2, int(args.min_sources))
        min_n = 1 if args.include_single else args.min_stations
        sel = (clust_stats["n_sources"] >= ms) & (clust_stats["n_members"] >= min_n)
        candidates = clust_stats[sel].index.tolist()
        print("  候选 cluster（n_sources >= {} 且 n_members >= {}）: {}".format(
            ms, min_n, len(candidates)))
        if not candidates:
            print(
                "Error: 没有同时满足多数据源与站点数条件的 cluster。"
                "可尝试: 降低 --min-sources / --min-stations，或加 --no-multi-source-filter"
            )
            return 1

    n_pick = min(args.n, len(candidates))
    if args.random_sample:
        rng = np.random.default_rng(args.seed)
        sampled = sorted(
            rng.choice(np.asarray(candidates, dtype=object), size=n_pick, replace=False).tolist()
        )
        print("  随机抽 {} 个 cluster（seed={}）: {}".format(n_pick, args.seed, sampled))
    else:
        tmp = clust_stats.loc[candidates].copy().reset_index()
        cid_col = tmp.columns[0]
        tmp = tmp.sort_values(
            by=["n_sources", "n_members", cid_col],
            ascending=[False, False, True],
        )
        sampled = tmp[cid_col].head(n_pick).tolist()
        print("  按 n_sources 降序取前 {} 个 cluster: {}".format(n_pick, sampled))

    # ── 打印抽查摘要 ──────────────────────────────────────────────────────
    print("\n{:<10} {:<8} {:<6} {:<12} {:<8} {}".format(
        "cluster_id", "n_mem", "n_src", "basin_area", "pfaf", "sources"))
    for cid in sampled:
        sub  = df[df["cluster_id"] == cid]
        ba   = sub["basin_area"].iloc[0] if "basin_area" in sub.columns else np.nan
        pfaf = sub["pfaf_code"].iloc[0]  if "pfaf_code"  in sub.columns else np.nan
        srcs = "|".join(sorted(sub["source"].unique()))
        nsrc = int(sub["n_sources"].iloc[0]) if "n_sources" in sub.columns else len(sub["source"].unique())
        print("  {:<10} {:<8} {:<6} {:<12} {:<8} {}".format(
            cid, len(sub), nsrc,
            "{:.0f}".format(ba) if pd.notna(ba) else "N/A",
            str(int(pfaf)) if pd.notna(pfaf) else "N/A",
            srcs,
        ))

    # ── 静态地图 ──────────────────────────────────────────────────────────
    if not args.no_static:
        try:
            out_png = out_dir / "s5_cluster_check.png"
            plot_static(
                sampled, df, out_png,
                plain_only=args.plain_only,
                random_sample=args.random_sample,
            )
        except Exception as e:
            print("Warning: 静态地图生成失败: {}".format(e))

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
