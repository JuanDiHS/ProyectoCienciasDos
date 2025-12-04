# scripts/run_analysis.py
import pickle, gzip, os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
proc = ROOT / "data" / "processed" / "run_auto"

def load_pickle_maybe_gz(p):
    p = Path(p)
    if not p.exists():
        raise FileNotFoundError(p)
    if p.suffix == ".gz":
        with gzip.open(p, "rb") as fh:
            return pickle.load(fh)
    else:
        with open(p, "rb") as fh:
            return pickle.load(fh)

print("Ruta data processed:", proc)
graph_path_candidates = [proc/"gtfs_graph.pkl.gz", proc/"gtfs_graph.pkl"]
bpt_path_candidates = [proc/"gtfs_bpt.pkl.gz", proc/"gtfs_bpt.pkl"]

gpath = next((p for p in graph_path_candidates if p.exists()), None)
bpt_path = next((p for p in bpt_path_candidates if p.exists()), None)

if gpath is None:
    raise SystemExit("No encontré gtfs_graph.pkl(.gz) en " + str(proc))
print("Cargando grafo desde", gpath)
G = load_pickle_maybe_gz(gpath)

if bpt_path:
    print("Cargando B+Tree desde", bpt_path)
    BPT = load_pickle_maybe_gz(bpt_path)
else:
    BPT = None
    print("No se encontró B+Tree (continuaré sin heurística)")

# Algoritmos
from transport_opt.graph.algorithms import shortest_path, astar, default_heuristic_factory, kruskal_mst, build_stops_info_from_bpt

# Elige dos stops de ejemplo (ajusta si no existen)
# Si no sabes stops, toma los primeros dos nodos:
nodes = list(G.nodes)
src = nodes[0]
dst = nodes[1] if len(nodes) > 1 else nodes[0]

print("Ejemplo nodes:", src, dst)

# Dijkstra
path_dij, cost_dij = shortest_path(G, src, dst)
print("Dijkstra -> costo:", cost_dij, "path len:", len(path_dij) if path_dij else 0)

# A* con heurística (si bpt disponible)
if BPT is not None:
    stops_info = build_stops_info_from_bpt(BPT)
    heur = default_heuristic_factory(stops_info)
    path_astar, cost_astar = astar(G, src, dst, heuristic=heur)
    print("A* (heur) -> costo:", cost_astar, "path len:", len(path_astar) if path_astar else 0)
else:
    path_astar, cost_astar = astar(G, src, dst)
    print("A* (sin heur) -> costo:", cost_astar, "path len:", len(path_astar) if path_astar else 0)

# MST (muestra)
mst, tw = kruskal_mst(G)
print("MST aristas (ejemplo primeras 10):", [(e.u,e.v,e.w) for e in mst[:10]])
print("MST peso total (ejemplo):", tw)

# Si tienes networkx/matplotlib, visualizar la ruta (opcional):
try:
    from transport_opt.viz.visualizer import visualize_path
    print("Visualizando ruta con networkx/matplotlib (si instalados)...")
    visualize_path(G, path_astar, title=f"Ruta {src} → {dst}")
except Exception as ex:
    print("No se puede visualizar (falta networkx/matplotlib o hay error):", ex)

print("Análisis rápido finalizado.")
