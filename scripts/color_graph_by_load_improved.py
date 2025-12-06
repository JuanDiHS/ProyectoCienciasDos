# scripts/color_graph_by_load_improved.py
"""
  python scripts/color_graph_by_load_improved.py --run run_auto --edge-load results/run_sim1/edge_load_named.csv --out outputs/graph_load_top100.png --top 100 --use-geo True
"""
from pathlib import Path
import argparse
import csv
import math

import matplotlib as mpl
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np

# imports del paquete (load_graph_from_processed se usa para reconstruir si hace falta)
from transport_opt.io import load_graph_from_processed

# Intentar importar utilidad para nombres, si no existe usamos fallback local
try:
    from transport_opt.utils import load_stop_names
except Exception:
    load_stop_names = None  # fallback más abajo

def read_edge_load_csv(path: Path):
    """Lee edge_load_named.csv (u_id,u_name,v_id,v_name,count) y devuelve dict (u,v)->count"""
    d = {}
    with open(path, newline='', encoding='utf-8') as fh:
        rdr = csv.DictReader(fh)
        for r in rdr:
            u = r.get("u_id") or r.get("u")
            v = r.get("v_id") or r.get("v")
            try:
                cnt = int(float(r.get("count") or 0))
            except Exception:
                cnt = 0
            if u and v:
                d[(u, v)] = cnt
    return d

def graph_to_networkx(g, edge_filter=None):
    """Convierte tu Graph ligero a networkx.Graph (sin metadata salvo peso)."""
    G = nx.Graph() if not g.directed else nx.DiGraph()
    for n in g.nodes:
        G.add_node(n)
    for u, nbrs in g.adj.items():
        for v, w in nbrs:
            if edge_filter is not None and (u, v) not in edge_filter and (v, u) not in edge_filter:
                continue
            if G.has_edge(u, v):
                prev = G[u][v].get("weight", float("inf"))
                if float(w) < prev:
                    G[u][v]["weight"] = float(w)
            else:
                G.add_edge(u, v, weight=float(w))
    return G

def load_stop_positions(processed_dir: Path):
    """Intenta leer gtfs_stops.csv(.gz) y devolver dict id -> (lon, lat)."""
    candidates = [
        processed_dir / "gtfs_stops.csv",
        processed_dir / "gtfs_stops.csv.gz",
    ]
    for p in candidates:
        if p.exists():
            import gzip
            opener = gzip.open if p.suffixes and p.suffixes[-1] == '.gz' else open
            with opener(p, mode='rt', encoding='utf-8') as fh:
                rdr = csv.DictReader(fh)
                pos = {}
                for r in rdr:
                    sid = r.get("stop_id") or r.get("id")
                    lat = r.get("stop_lat") or r.get("lat")
                    lon = r.get("stop_lon") or r.get("lon")
                    try:
                        latf = float(lat) if lat not in (None,"") else None
                        lonf = float(lon) if lon not in (None,"") else None
                    except Exception:
                        latf = lonf = None
                    if sid and latf is not None and lonf is not None:
                        pos[sid] = (lonf, latf)  # (x,y) = (lon,lat)
            if pos:
                return pos
    return {}

def load_stop_names_from_csv(processed_dir: Path):
    """Lee gtfs_stops.csv(.gz) y devuelve dict stop_id -> stop_name"""
    candidates = [
        processed_dir / "gtfs_stops.csv",
        processed_dir / "gtfs_stops.csv.gz",
    ]
    for p in candidates:
        if p.exists():
            import gzip
            opener = gzip.open if p.suffixes and p.suffixes[-1] == '.gz' else open
            with opener(p, mode='rt', encoding='utf-8') as fh:
                rdr = csv.DictReader(fh)
                names = {}
                for r in rdr:
                    sid = r.get("stop_id") or r.get("id")
                    name = r.get("stop_name") or r.get("name") or r.get("metadata")
                    if sid:
                        names[sid] = name or sid
                return names
    return {}

def ensure_stop_names(processed_dir: Path, bpt):
    """Obtiene stop_name map usando (en orden): transport_opt.utils.load_stop_names, bpt, csv, fallback id->id"""
    # 1) if function from utils exists, try to use it
    if callable(load_stop_names):
        try:
            names = load_stop_names(processed_dir, bpt=bpt)
            if names:
                return names
        except Exception:
            pass
    # 2) try B+Tree if provided (traverse_leaves)
    if bpt is not None:
        try:
            leaves = bpt.traverse_leaves()
            names = {}
            for sid, meta in leaves:
                if isinstance(meta, dict):
                    names[str(sid)] = meta.get('name') or meta.get('stop_name') or str(sid)
                else:
                    names[str(sid)] = str(sid)
            if names:
                return names
        except Exception:
            pass
    # 3) try csv file
    names = load_stop_names_from_csv(processed_dir)
    if names:
        return names
    # fallback: identity mapping
    return {}

def plot_subgraph(G_nx, edge_load, stop_pos, stop_names, out_path: Path, top=100, show_labels=False, cmap_name="viridis", dpi=300, figsize=(12,10), max_edge_width=6):
    # select top edges by count
    sorted_edges = sorted(edge_load.items(), key=lambda kv: kv[1], reverse=True)
    top_edges = [kv[0] for kv in sorted_edges[:top]] if top>0 else [k for k in edge_load.keys()]

    # build subgraph containing only nodes in top edges
    nodes_set = set()
    for u,v in top_edges:
        nodes_set.add(u); nodes_set.add(v)
    sub = G_nx.subgraph(nodes_set).copy()

    if len(sub.nodes()) == 0:
        print("No hay nodos en el subgrafo (revisa edge_load o top).")
        return

    # prepare positions: use stop_pos if available, else spring_layout
    if stop_pos and all(n in stop_pos for n in sub.nodes()):
        pos = {n: stop_pos[n] for n in sub.nodes()}
        xs = [p[0] for p in pos.values()]; ys = [p[1] for p in pos.values()]
        minx, maxx = min(xs), max(xs)
        miny, maxy = min(ys), max(ys)
        spanx = maxx - minx if maxx>minx else 1.0
        spany = maxy - miny if maxy>miny else 1.0
        pos = {n: ((lon - minx)/spanx, (lat - miny)/spany) for n,(lon,lat) in pos.items()}
    else:
        pos = nx.spring_layout(sub, seed=42, k=0.1, iterations=150)

    # gather edge colors/widths
    counts = []
    edges_to_draw = []
    for (u,v) in sub.edges():
        cnt = edge_load.get((u,v), edge_load.get((v,u), 0))
        counts.append(cnt)
        edges_to_draw.append((u,v))

    if not counts:
        print("No hay aristas para dibujar.")
        return

    counts_arr = np.array(counts, dtype=float)
    norm = mpl.colors.Normalize(vmin=counts_arr.min(), vmax=counts_arr.max())
    cmap = mpl.cm.get_cmap(cmap_name)

    edge_colors = [cmap(norm(c)) for c in counts_arr]
    maxc = counts_arr.max() if counts_arr.max()>0 else 1.0
    widths = [(max_edge_width * (c / maxc)) + 0.5 for c in counts_arr]

    plt.figure(figsize=figsize, dpi=dpi)
    ax = plt.gca()
    nx.draw_networkx_nodes(sub, pos, node_size=20, node_color="#222222", alpha=0.9, ax=ax)
    nx.draw_networkx_edges(sub, pos, edgelist=edges_to_draw, width=widths, edge_color=edge_colors, alpha=0.9, ax=ax)

    if show_labels:
        labels = {n: stop_names.get(n, n) for n in sub.nodes()}
        nx.draw_networkx_labels(sub, pos, labels=labels, font_size=7, ax=ax)

    sm = mpl.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array(counts_arr)
    cbar = plt.colorbar(sm, ax=ax, fraction=0.046, pad=0.02)
    cbar.set_label("conteo / carga (sim)")

    plt.axis('off')
    plt.title(f"Top {len(edges_to_draw)} aristas por carga")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=dpi)
    plt.close()
    print("Saved visualization to", out_path)

def main(argv=None):
    argv = argv or None
    p = argparse.ArgumentParser()
    p.add_argument("--run", default="run_auto")
    p.add_argument("--root", default=".")
    p.add_argument("--edge-load", required=True, help="CSV generado por run_simulation (edge_load_named.csv)")
    p.add_argument("--out", required=True, help="PNG destino")
    p.add_argument("--top", type=int, default=100)
    p.add_argument("--labels", type=lambda s: s.lower() in ("1","true","yes"), default=False)
    p.add_argument("--use-geo", type=lambda s: s.lower() in ("1","true","yes"), default=True)
    args = p.parse_args(argv)

    ROOT = Path(args.root)
    processed_dir = ROOT / "data" / "processed" / args.run

    # cargar grafo (reconstruye si pickle falla)
    graph, bpt = load_graph_from_processed(ROOT, run_name=args.run)
    print("Grafo cargado. Nodos:", len(graph.nodes))

    edge_load_path = Path(args.edge_load)
    if not edge_load_path.exists():
        raise SystemExit("edge_load file no encontrado: " + str(edge_load_path))
    edge_load = read_edge_load_csv(edge_load_path)

    # generar networkx (filtrando por algunas llaves para eficiencia)
    sorted_keys = sorted(edge_load.items(), key=lambda kv: kv[1], reverse=True)
    top_keys = [kv[0] for kv in sorted_keys[:max(1, args.top*3)]]
    G_nx = graph_to_networkx(graph, edge_filter=set(top_keys))

    # posiciones geográficas si se piden
    stop_pos = {}
    if args.use_geo:
        stop_pos = load_stop_positions(processed_dir)

    # nombres legibles
    stop_names = ensure_stop_names(processed_dir, bpt)

    plot_subgraph(G_nx, edge_load, stop_pos, stop_names, Path(args.out), top=args.top, show_labels=args.labels)

if __name__ == "__main__":
    main()
